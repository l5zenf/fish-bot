use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::quote;
use syn::{
    FnArg, Ident, ImplItem, ItemImpl, LitStr, Token, Type,
    parse::ParseStream,
    parse_macro_input,
    spanned::Spanned,
};

// ---- Exported proc macros ----

fn runtime_path() -> proc_macro2::TokenStream {
    match crate_name("fish-runtime") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = proc_macro2::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_) => quote!(::fish_runtime),
    }
}

fn extract_type_ident(ty: &Type) -> Option<Ident> {
    match ty {
        Type::Path(type_path) => type_path.path.segments.last().map(|segment| segment.ident.clone()),
        _ => None,
    }
}

/// Attribute macro on the plugin's impl block:
/// `#[plugin]` or `#[plugin(Self::new())]`
///
/// Parses handler method attributes and generates the `Plugin` trait implementation.
#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let meta = parse_macro_input!(attr as PluginMeta);
    let mut impl_block = parse_macro_input!(item as ItemImpl);
    let struct_name = &impl_block.self_ty;
    let runtime = runtime_path();

    let type_ident = match extract_type_ident(struct_name) {
        Some(ident) => ident,
        None => {
            return syn::Error::new(
                struct_name.span(),
                "#[plugin] only supports inherent impl blocks on concrete types",
            )
            .to_compile_error()
            .into();
        }
    };

    let id = to_snake_case(&type_ident.to_string());
    let name = type_ident.to_string();
    let version = "1.0.0".to_string();
    let description = String::new();
    let author = "Unknown".to_string();
    let init_expr = meta.init.unwrap_or_else(|| quote!(Self));

    // Collect handler methods, keeping them in the output (with custom attrs stripped)
    let mut msg_exprs = Vec::new();
    let mut event_exprs = Vec::new();
    let mut kept_items: Vec<ImplItem> = Vec::new();

    for item in impl_block.items.drain(..) {
        match item {
            ImplItem::Fn(mut method) => {
                let handler_ident = method.sig.ident.to_string();

                let attr_names: Vec<usize> = method
                    .attrs
                    .iter()
                    .enumerate()
                    .filter(|(_, a)| {
                        a.path().is_ident("command")
                            || a.path().is_ident("message")
                            || a.path().is_ident("event")
                    })
                    .map(|(i, _)| i)
                    .collect();

                if attr_names.len() == 1 {
                    let idx = attr_names[0];
                    let handler_attr = method.attrs.swap_remove(idx);

                    // Strip custom attributes from the output method
                    method.attrs.retain(|a| {
                        !a.path().is_ident("command")
                            && !a.path().is_ident("message")
                            && !a.path().is_ident("event")
                    });

                    // Detect receiver kind
                    let receiver = detect_receiver(&method.sig.inputs);
                    if matches!(receiver, ReceiverKind::None | ReceiverKind::Owned) {
                        return syn::Error::new(
                            method.sig.ident.span(),
                            "macro-based plugin handlers must use `&self` or `&mut self`; use ActorPluginBuilder for actor-first plugins",
                        )
                        .to_compile_error()
                        .into();
                    }

                    if handler_attr.path().is_ident("command")
                        || handler_attr.path().is_ident("message")
                    {
                        let msg = parse_message_attr(&handler_attr).unwrap_or_else(|err| {
                            panic!(
                                "invalid #[{}(...)] attribute: {err}",
                                handler_attr.path().get_ident().unwrap()
                            )
                        });
                        let expr = gen_message_handler(
                            &runtime,
                            struct_name,
                            &handler_ident,
                            &msg,
                            receiver,
                        );
                        msg_exprs.push(expr);
                    } else if handler_attr.path().is_ident("event") {
                        let evt = parse_event_attr(&handler_attr).expect("invalid #[event(...)]");
                        let expr = gen_event_handler(
                            &runtime,
                            struct_name,
                            &handler_ident,
                            &evt,
                            receiver,
                        );
                        event_exprs.push(expr);
                    }

                    kept_items.push(ImplItem::Fn(method));
                } else if attr_names.is_empty() {
                    kept_items.push(ImplItem::Fn(method));
                }
                // More than one handler attr — skip (user error, let compiler report it)
            }
            other => kept_items.push(other),
        }
    }

    impl_block.items = kept_items;

    let expanded = quote! {
        #[doc(hidden)]
        impl #struct_name {
            fn __fish_plugin_create_initial_state() -> Self { #init_expr }
        }

        #[doc(hidden)]
        mod __fish_plugin_meta {
            #![allow(non_upper_case_globals, dead_code)]
            use super::*;

            pub(super) static METADATA: std::sync::LazyLock<
                #runtime::PluginMetadata
            > = std::sync::LazyLock::new(|| {
                #runtime::PluginMetadata {
                    id: String::from(#id),
                    name: String::from(#name),
                    description: String::from(#description),
                    version: String::from(#version),
                    author: String::from(#author),
                }
            });
        }

        impl #runtime::Plugin for #struct_name {
            fn metadata(&self) -> &#runtime::PluginMetadata {
                &__fish_plugin_meta::METADATA
            }

            fn initial_state(&self) -> Option<#runtime::PluginState> {
                Some(std::sync::Arc::new(tokio::sync::RwLock::new(
                    #struct_name::__fish_plugin_create_initial_state(),
                )))
            }

            fn message_handlers(&self) -> &[#runtime::MessageHandler] {
                static HANDLERS: std::sync::LazyLock<Vec<#runtime::MessageHandler>> =
                    std::sync::LazyLock::new(|| {
                        vec![
                            #(#msg_exprs),*
                        ]
                    });
                &HANDLERS
            }

            fn event_handlers(&self) -> &[#runtime::EventHandler] {
                static HANDLERS: std::sync::LazyLock<Vec<#runtime::EventHandler>> =
                    std::sync::LazyLock::new(|| {
                        vec![
                            #(#event_exprs),*
                        ]
                    });
                &HANDLERS
            }
        }

        #impl_block
    };

    TokenStream::from(expanded)
}

// ---- Handler generation ----

fn gen_message_handler(
    runtime: &proc_macro2::TokenStream,
    struct_name: &syn::Type,
    handler_id: &str,
    route: &MessageHandlerData,
    receiver: ReceiverKind,
) -> proc_macro2::TokenStream {
    let hid = handler_id;
    let pattern = &route.pattern_value;
    let method_name = Ident::new(handler_id, proc_macro2::Span::call_site());

    // Build the Arc<|cx| { ... }> closure for each receiver kind
    let closure_body = match receiver {
        ReceiverKind::MutRef => {
            quote! {
                std::sync::Arc::new(move |cx: #runtime::HandlerContext| {
                    Box::pin(async move {
                        let mut plugin = cx.state_write::<#struct_name>().await?;
                        let event = cx.event;
                        let adapter = cx.adapter;
                        let app_ctx = cx.app_ctx;
                        let telemetry = cx.telemetry;
                        let ctx = #runtime::MessageContext::new(event, adapter, app_ctx, telemetry);
                        plugin.#method_name(ctx).await
                    })
                })
            }
        }
        ReceiverKind::Ref => {
            quote! {
                std::sync::Arc::new(move |cx: #runtime::HandlerContext| {
                    Box::pin(async move {
                        let plugin = cx.state_read::<#struct_name>().await?;
                        let event = cx.event;
                        let adapter = cx.adapter;
                        let app_ctx = cx.app_ctx;
                        let telemetry = cx.telemetry;
                        let ctx = #runtime::MessageContext::new(event, adapter, app_ctx, telemetry);
                        plugin.#method_name(ctx).await
                    })
                })
            }
        }
        ReceiverKind::None | ReceiverKind::Owned => unreachable!("validated earlier"),
    };

    let kind = route.kind.as_deref().unwrap_or("exact");
    match kind {
        "prefix" => {
            quote! { #runtime::MessageHandler::prefix(#hid, vec![#pattern], #closure_body) }
        }
        "regex" => {
            quote! { #runtime::MessageHandler::regex(#hid, #pattern, #closure_body) }
        }
        "fallback" => {
            quote! { #runtime::MessageHandler::fallback(#hid, #closure_body) }
        }
        "keyword" => {
            quote! { #runtime::MessageHandler::keyword(#hid, vec![#pattern], #closure_body) }
        }
        "exact" => {
            quote! { #runtime::MessageHandler::exact(#hid, vec![#pattern], #closure_body) }
        }
        other => panic!("unsupported message kind: {other}"),
    }
}

fn gen_event_handler(
    runtime: &proc_macro2::TokenStream,
    struct_name: &syn::Type,
    handler_id: &str,
    evt: &EventHandlerData,
    receiver: ReceiverKind,
) -> proc_macro2::TokenStream {
    let hid = handler_id;
    let event_type = &evt.event_type;
    let method_name = Ident::new(handler_id, proc_macro2::Span::call_site());

    let closure_body = match receiver {
        ReceiverKind::MutRef => {
            quote! {
                Box::pin(async move {
                    let mut plugin = cx.state_write::<#struct_name>().await?;
                    let event = cx.event;
                    let adapter = cx.adapter;
                    let app_ctx = cx.app_ctx;
                    let telemetry = cx.telemetry;
                    let ctx = #runtime::EventContext::new(
                        event, adapter, app_ctx, telemetry,
                    );
                    plugin.#method_name(ctx).await
                })
            }
        }
        ReceiverKind::Ref => {
            quote! {
                Box::pin(async move {
                    let plugin = cx.state_read::<#struct_name>().await?;
                    let event = cx.event;
                    let adapter = cx.adapter;
                    let app_ctx = cx.app_ctx;
                    let telemetry = cx.telemetry;
                    let ctx = #runtime::EventContext::new(
                        event, adapter, app_ctx, telemetry,
                    );
                    plugin.#method_name(ctx).await
                })
            }
        }
        ReceiverKind::None | ReceiverKind::Owned => unreachable!("validated earlier"),
    };

    quote! {
        #runtime::EventHandler::new(
            #event_type,
            #hid,
            std::sync::Arc::new(move |cx: #runtime::EventHandlerContext| { #closure_body }),
        )
    }
}

// ---- Helper types for handler attributes ----

struct MessageHandlerData {
    pattern_value: String,
    kind: Option<String>,
}

fn parse_message_attr(attr: &syn::Attribute) -> syn::Result<MessageHandlerData> {
    attr.parse_args_with(|input: ParseStream| {
        // #[message("/ping")] or #[message("/admin", kind = "prefix")]
        if input.peek(LitStr) && !input.peek(Token![=]) {
            let pattern: LitStr = input.parse()?;
            let mut kind = "exact".to_string();
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
                while !input.is_empty() {
                    let key: Ident = input.parse()?;
                    let _: Token![=] = input.parse()?;
                    let value: LitStr = input.parse()?;
                    match key.to_string().as_str() {
                        "kind" => kind = value.value(),
                        _ => return Err(syn::Error::new(key.span(), "unknown message key")),
                    }
                    if input.peek(Token![,]) {
                        let _: Token![,] = input.parse()?;
                    }
                }
            }
            return Ok(MessageHandlerData {
                pattern_value: pattern.value(),
                kind: Some(kind),
            });
        }

        if !input.peek(Ident) {
            return Err(syn::Error::new(
                input.span(),
                "expected pattern string or `fallback`",
            ));
        }

        // #[message(fallback)] or #[message(pattern = "...", kind = "regex")]
        let mut pattern = String::new();
        let mut kind = "exact".to_string();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let key_str = key.to_string();

            if key_str == "fallback" {
                return Ok(MessageHandlerData {
                    pattern_value: String::new(),
                    kind: Some("fallback".into()),
                });
            }

            let _: Token![=] = input.parse()?;
            let value: LitStr = input.parse()?;
            match key_str.as_str() {
                "pattern" => pattern = value.value(),
                "keyword" => {
                    pattern = value.value();
                    kind = "keyword".into();
                }
                "kind" => kind = value.value(),
                _ => return Err(syn::Error::new(key.span(), "unknown message key")),
            }
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }

        if kind != "fallback" && pattern.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "#[message(...)] requires a pattern, keyword, or fallback",
            ));
        }

        Ok(MessageHandlerData {
            pattern_value: pattern,
            kind: Some(kind),
        })
    })
}

struct EventHandlerData {
    event_type: String,
}

fn parse_event_attr(attr: &syn::Attribute) -> syn::Result<EventHandlerData> {
    let event_type: LitStr = attr.parse_args()?;
    Ok(EventHandlerData {
        event_type: event_type.value(),
    })
}

// ---- Detect receiver type ----

#[derive(Clone, Copy, PartialEq)]
enum ReceiverKind {
    None,
    Owned,
    Ref,
    MutRef,
}

fn detect_receiver(inputs: &syn::punctuated::Punctuated<FnArg, Token![,]>) -> ReceiverKind {
    let first = match inputs.first() {
        Some(f) => f,
        None => return ReceiverKind::None,
    };
    match first {
        FnArg::Receiver(receiver) => {
            if receiver.reference.is_some() {
                if receiver.mutability.is_some() {
                    ReceiverKind::MutRef
                } else {
                    ReceiverKind::Ref
                }
            } else {
                ReceiverKind::Owned
            }
        }
        FnArg::Typed(_) => ReceiverKind::None,
    }
}

// ---- PluginMeta ----

#[derive(Default)]
struct PluginMeta {
    init: Option<proc_macro2::TokenStream>,
}

impl syn::parse::Parse for PluginMeta {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self::default());
        }

        let expr = input.parse::<proc_macro2::TokenStream>()?;
        if expr.is_empty() {
            return Ok(Self::default());
        }

        Ok(Self { init: Some(expr) })
    }
}

fn to_snake_case(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len() + 4);

    for (index, &ch) in chars.iter().enumerate() {
        if ch.is_uppercase() {
            let has_prev = index > 0;
            let prev_is_lower_or_digit = has_prev
                && chars[index - 1]
                    .is_ascii_lowercase()
                    || has_prev && chars[index - 1].is_ascii_digit();
            let next_is_lower = chars
                .get(index + 1)
                .map(|next| next.is_ascii_lowercase())
                .unwrap_or(false);

            if has_prev && (prev_is_lower_or_digit || next_is_lower) {
                out.push('_');
            }

            for lower in ch.to_lowercase() {
                out.push(lower);
            }
        } else {
            out.push(ch);
        }
    }

    out
}
