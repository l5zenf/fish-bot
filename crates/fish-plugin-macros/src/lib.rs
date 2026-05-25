use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::quote;
use syn::{
    FnArg, Ident, ImplItem, ItemImpl, LitStr, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

// ---- Exported proc macros ----

/// Extract the struct identifier from a token stream representing a struct item.
fn extract_struct_ident(tokens: &proc_macro2::TokenStream) -> Ident {
    let mut iter = tokens.clone().into_iter().peekable();
    while let Some(token) = iter.next() {
        if let proc_macro2::TokenTree::Ident(i) = &token {
            if i == "struct" {
                if let Some(proc_macro2::TokenTree::Ident(name)) = iter.next() {
                    return name;
                }
            }
        }
    }
    panic!("no struct definition found in #[plugin] item");
}

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

/// Attribute macro on the plugin struct: `#[plugin(id = "x", name = "y")]`
///
/// Stores plugin metadata in a hidden module so `#[plugin_handlers]` can reference it.
/// Also generates `create_initial_state()` which `#[plugin_handlers]` uses for state
/// initialization. Supports `init = "Type::new()"` for custom initialization, otherwise
/// uses `Default::default()`.
#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let meta = parse_macro_input!(attr as PluginMeta);
    let runtime = runtime_path();

    let item: proc_macro2::TokenStream = item.into();
    let struct_ident = extract_struct_ident(&item);

    let id = meta.id;
    let name = meta.name;
    let version = meta.version;
    let description = meta.description;
    let author = meta.author;

    let init_fn = match &meta.init {
        Some(expr_str) => {
            let expr: proc_macro2::TokenStream = expr_str
                .parse()
                .expect("invalid init expression in #[plugin]");
            quote! {
                #[doc(hidden)]
                #[allow(non_upper_case_globals)]
                fn __fish_plugin_create_initial_state() -> #struct_ident { #expr }
            }
        }
        None => {
            quote! {
                #[doc(hidden)]
                #[allow(non_upper_case_globals)]
                fn __fish_plugin_create_initial_state() -> #struct_ident { #struct_ident::default() }
            }
        }
    };

    let expanded = quote! {
        #item

        #init_fn

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
    };

    TokenStream::from(expanded)
}

/// Attribute macro on the plugin's impl block: `#[plugin_handlers]`
///
/// Parses handler method attributes and generates the `Plugin` trait implementation.
#[proc_macro_attribute]
pub fn plugin_handlers(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut impl_block = parse_macro_input!(item as ItemImpl);
    let struct_name = &impl_block.self_ty;
    let runtime = runtime_path();

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

                    if handler_attr.path().is_ident("command") {
                        let cmd =
                            parse_command_attr(&handler_attr).expect("invalid #[command(...)]");
                        let expr = gen_message_handler(
                            &runtime,
                            struct_name,
                            &handler_ident,
                            &cmd,
                            receiver,
                            false,
                        );
                        msg_exprs.push(expr);
                    } else if handler_attr.path().is_ident("message") {
                        let msg =
                            parse_message_attr(&handler_attr).expect("invalid #[message(...)]");
                        let expr = gen_message_handler(
                            &runtime,
                            struct_name,
                            &handler_ident,
                            &msg,
                            receiver,
                            true,
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
        impl #runtime::Plugin for #struct_name {
            fn metadata(&self) -> &#runtime::PluginMetadata {
                &__fish_plugin_meta::METADATA
            }

            fn __initial_state(&self) -> Option<std::sync::Arc<dyn std::any::Any + Send + Sync>> {
                Some(std::sync::Arc::new(tokio::sync::RwLock::new(
                    __fish_plugin_create_initial_state(),
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

            fn event_handlers(&self) -> std::collections::HashMap<String, Vec<#runtime::EventHandler>> {
                let mut map = std::collections::HashMap::new();
                #(#event_exprs)*
                map
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
    cmd: &MessageHandlerData,
    receiver: ReceiverKind,
    is_keyword: bool,
) -> proc_macro2::TokenStream {
    let hid = handler_id;
    let pattern = &cmd.pattern_value;
    let method_name = Ident::new(handler_id, proc_macro2::Span::call_site());

    // Build the Arc<|cx| { ... }> closure for each receiver kind
    let closure_body = match receiver {
        ReceiverKind::MutRef => {
            quote! {
                std::sync::Arc::new(move |cx: #runtime::HandlerContext| {
                    let lock = #runtime::__state_lock_tokio::<#struct_name>(cx.__plugin_state());
                    let event = cx.event;
                    let adapter = cx.adapter;
                    let app_ctx = cx.app_ctx;
                    let telemetry = cx.telemetry;
                    Box::pin(async move {
                        let mut plugin = lock.write().await;
                        let ctx = #runtime::Context::new(event, adapter, app_ctx, telemetry);
                        plugin.#method_name(ctx).await
                    })
                })
            }
        }
        ReceiverKind::Ref => {
            quote! {
                std::sync::Arc::new(move |cx: #runtime::HandlerContext| {
                    let lock = #runtime::__state_lock_tokio::<#struct_name>(cx.__plugin_state());
                    let event = cx.event;
                    let adapter = cx.adapter;
                    let app_ctx = cx.app_ctx;
                    let telemetry = cx.telemetry;
                    Box::pin(async move {
                        let plugin = lock.read().await;
                        let ctx = #runtime::Context::new(event, adapter, app_ctx, telemetry);
                        plugin.#method_name(ctx).await
                    })
                })
            }
        }
        ReceiverKind::None | ReceiverKind::Owned => {
            quote! {
                std::sync::Arc::new(move |cx: #runtime::HandlerContext| {
                    let event = cx.event;
                    let adapter = cx.adapter;
                    let app_ctx = cx.app_ctx;
                    let telemetry = cx.telemetry;
                    Box::pin(async move {
                        let ctx = #runtime::Context::new(event, adapter, app_ctx, telemetry);
                        #struct_name::#method_name(ctx).await
                    })
                })
            }
        }
    };

    let kind = cmd.kind.as_deref().unwrap_or("exact");
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
        _ if is_keyword => {
            quote! { #runtime::MessageHandler::keyword(#hid, vec![#pattern], #closure_body) }
        }
        _ => {
            quote! { #runtime::MessageHandler::exact(#hid, vec![#pattern], #closure_body) }
        }
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
                    let lock = #runtime::__state_lock_tokio::<#struct_name>(cx.__plugin_state());
                    let mut plugin = lock.write().await;
                    let ctx = #runtime::Context::new_from_event(
                        cx.event, cx.adapter, cx.app_ctx,
                    );
                    plugin.#method_name(ctx).await
                })
            }
        }
        ReceiverKind::Ref => {
            quote! {
                Box::pin(async move {
                    let lock = #runtime::__state_lock_tokio::<#struct_name>(cx.__plugin_state());
                    let plugin = lock.read().await;
                    let ctx = #runtime::Context::new_from_event(
                        cx.event, cx.adapter, cx.app_ctx,
                    );
                    plugin.#method_name(ctx).await
                })
            }
        }
        ReceiverKind::None | ReceiverKind::Owned => {
            quote! {
                Box::pin(async move {
                    let ctx = #runtime::Context::new_from_event(
                        cx.event, cx.adapter, cx.app_ctx,
                    );
                    #struct_name::#method_name(ctx).await
                })
            }
        }
    };

    quote! {
        map.insert(
            String::from(#event_type),
            vec![
                #runtime::EventHandler::new(
                    #hid,
                    std::sync::Arc::new(move |cx: #runtime::EventHandlerContext|
                    { #closure_body }),
                ),
            ],
        );
    }
}

// ---- Helper types for handler attributes ----

struct MessageHandlerData {
    pattern_value: String,
    kind: Option<String>,
}

fn parse_command_attr(attr: &syn::Attribute) -> syn::Result<MessageHandlerData> {
    attr.parse_args_with(|input: ParseStream| {
        // #[command("/ping")] or #[command("/admin", kind = "prefix")]
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
                        _ => return Err(syn::Error::new(key.span(), "unknown command key")),
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

        // #[command(fallback)] or #[command(pattern = "...", kind = "regex")]
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
                "kind" => kind = value.value(),
                _ => return Err(syn::Error::new(key.span(), "unknown command key")),
            }
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }
        Ok(MessageHandlerData {
            pattern_value: pattern,
            kind: Some(kind),
        })
    })
}

fn parse_message_attr(attr: &syn::Attribute) -> syn::Result<MessageHandlerData> {
    attr.parse_args_with(|input: ParseStream| {
        let mut keyword = String::new();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            let value: LitStr = input.parse()?;
            match key.to_string().as_str() {
                "keyword" => keyword = value.value(),
                _ => return Err(syn::Error::new(key.span(), "unknown message key")),
            }
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }
        if keyword.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "#[message(...)] requires `keyword`",
            ));
        }
        Ok(MessageHandlerData {
            pattern_value: keyword,
            kind: None,
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
    id: String,
    name: String,
    version: String,
    description: String,
    author: String,
    init: Option<String>,
}

impl Parse for PluginMeta {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut meta = PluginMeta::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            let value: LitStr = input.parse()?;
            let s = value.value();
            match key.to_string().as_str() {
                "id" => meta.id = s,
                "name" => meta.name = s,
                "version" => meta.version = s,
                "description" => meta.description = s,
                "author" => meta.author = s,
                "init" => meta.init = Some(s),
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown plugin metadata key: {}", key),
                    ));
                }
            }
            if !input.is_empty() {
                let _: Token![,] = input.parse()?;
            }
        }
        if meta.id.is_empty() {
            return Err(syn::Error::new(input.span(), "plugin id is required"));
        }
        if meta.name.is_empty() {
            return Err(syn::Error::new(input.span(), "plugin name is required"));
        }
        Ok(meta)
    }
}
