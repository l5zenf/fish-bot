use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, FnArg, Ident, ImplItem, ItemImpl, LitStr, Token,
};

// ---- Exported proc macros ----

/// Attribute macro on the plugin struct: `#[plugin(id = "x", name = "y")]`
///
/// Stores plugin metadata in a hidden module so `#[plugin_handlers]` can reference it.
#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let meta = parse_macro_input!(attr as PluginMeta);
    let struct_item: proc_macro2::TokenStream = item.into();

    let id = meta.id;
    let name = meta.name;
    let version = meta.version;
    let description = meta.description;
    let author = meta.author;

    let expanded = quote! {
        #[derive(Default)]
        #struct_item

        #[doc(hidden)]
        mod __fish_plugin_meta {
            #![allow(non_upper_case_globals, dead_code)]
            use super::*;

            pub(super) static METADATA: std::sync::LazyLock<
                fish_plugin_sdk::PluginMetadata
            > = std::sync::LazyLock::new(|| {
                fish_plugin_sdk::PluginMetadata {
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
                        let cmd = parse_command_attr(&handler_attr)
                            .expect("invalid #[command(...)]");
                        let expr = gen_message_handler(
                            struct_name, &handler_ident, &cmd, receiver, false,
                        );
                        msg_exprs.push(expr);
                    } else if handler_attr.path().is_ident("message") {
                        let msg = parse_message_attr(&handler_attr)
                            .expect("invalid #[message(...)]");
                        let expr = gen_message_handler(
                            struct_name, &handler_ident, &msg, receiver, true,
                        );
                        msg_exprs.push(expr);
                    } else if handler_attr.path().is_ident("event") {
                        let evt = parse_event_attr(&handler_attr)
                            .expect("invalid #[event(...)]");
                        let expr = gen_event_handler(struct_name, &handler_ident, &evt, receiver);
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
        impl fish_plugin_sdk::Plugin for #struct_name {
            fn metadata(&self) -> &fish_plugin_sdk::PluginMetadata {
                &__fish_plugin_meta::METADATA
            }

            fn initial_state(&self) -> Option<std::sync::Arc<dyn std::any::Any + Send + Sync>> {
                Some(std::sync::Arc::new(tokio::sync::RwLock::new(Self::default())))
            }

            fn message_handlers(&self) -> &[fish_plugin_sdk::MessageHandler] {
                static HANDLERS: std::sync::LazyLock<Vec<fish_plugin_sdk::MessageHandler>> =
                    std::sync::LazyLock::new(|| {
                        vec![
                            #(#msg_exprs),*
                        ]
                    });
                &HANDLERS
            }

            fn event_handlers(&self) -> std::collections::HashMap<String, Vec<fish_plugin_sdk::EventHandler>> {
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
                std::sync::Arc::new(move |cx: fish_plugin_sdk::HandlerContext| {
                    let plugin_state = cx.plugin_state.clone()
                        .expect("stateful plugin: plugin_state is None");
                    let lock = plugin_state
                        .downcast::<tokio::sync::RwLock<#struct_name>>()
                        .expect("plugin_state type mismatch");
                    Box::pin(async move {
                        let mut plugin = lock.write().await;
                        let ctx = fish_plugin_sdk::Context::new(cx);
                        plugin.#method_name(ctx).await
                    })
                })
            }
        }
        ReceiverKind::Ref => {
            quote! {
                std::sync::Arc::new(move |cx: fish_plugin_sdk::HandlerContext| {
                    let plugin_state = cx.plugin_state.clone()
                        .expect("stateful plugin: plugin_state is None");
                    let lock = plugin_state
                        .downcast::<tokio::sync::RwLock<#struct_name>>()
                        .expect("plugin_state type mismatch");
                    Box::pin(async move {
                        let plugin = lock.read().await;
                        let ctx = fish_plugin_sdk::Context::new(cx);
                        plugin.#method_name(ctx).await
                    })
                })
            }
        }
        ReceiverKind::None | ReceiverKind::Owned => {
            quote! {
                std::sync::Arc::new(move |cx: fish_plugin_sdk::HandlerContext| {
                    Box::pin(async move {
                        let ctx = fish_plugin_sdk::Context::new(cx);
                        #struct_name::#method_name(ctx).await
                    })
                })
            }
        }
    };

    let kind = cmd.kind.as_deref().unwrap_or("exact");
    match kind {
        "prefix" => {
            quote! { fish_plugin_sdk::MessageHandler::prefix(#hid, vec![#pattern], #closure_body) }
        }
        "regex" => {
            quote! { fish_plugin_sdk::MessageHandler::regex(#hid, #pattern, #closure_body) }
        }
        "fallback" => {
            quote! { fish_plugin_sdk::MessageHandler::fallback(#hid, #closure_body) }
        }
        _ if is_keyword => {
            quote! { fish_plugin_sdk::MessageHandler::keyword(#hid, vec![#pattern], #closure_body) }
        }
        _ => {
            quote! { fish_plugin_sdk::MessageHandler::exact(#hid, vec![#pattern], #closure_body) }
        }
    }
}

fn gen_event_handler(
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
                let plugin_state = plugin_state
                    .expect("stateful plugin: plugin_state is None");
                Box::pin(async move {
                    let lock = plugin_state
                        .downcast::<tokio::sync::RwLock<#struct_name>>()
                        .expect("plugin_state type mismatch");
                    let mut plugin = lock.write().await;
                    let ctx = fish_plugin_sdk::Context::new_from_event(
                        event, adapter, ctx,
                    );
                    plugin.#method_name(ctx).await
                })
            }
        }
        ReceiverKind::Ref => {
            quote! {
                let plugin_state = plugin_state
                    .expect("stateful plugin: plugin_state is None");
                Box::pin(async move {
                    let lock = plugin_state
                        .downcast::<tokio::sync::RwLock<#struct_name>>()
                        .expect("plugin_state type mismatch");
                    let plugin = lock.read().await;
                    let ctx = fish_plugin_sdk::Context::new_from_event(
                        event, adapter, ctx,
                    );
                    plugin.#method_name(ctx).await
                })
            }
        }
        ReceiverKind::None | ReceiverKind::Owned => {
            quote! {
                Box::pin(async move {
                    let ctx = fish_plugin_sdk::Context::new_from_event(
                        event, adapter, ctx,
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
                fish_plugin_sdk::EventHandler::new(
                    #hid,
                    std::sync::Arc::new(move |event: std::sync::Arc<fish_plugin_sdk::SystemEvent>,
                                          adapter: std::sync::Arc<dyn fish_plugin_sdk::BaseAdapter>,
                                          ctx: std::sync::Arc<fish_plugin_sdk::Ctx>,
                                          plugin_state: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>|
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
            return Err(syn::Error::new(input.span(), "expected pattern string or `fallback`"));
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
            return Err(syn::Error::new(input.span(), "#[message(...)] requires `keyword`"));
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
                _ => return Err(syn::Error::new(key.span(), format!("unknown plugin metadata key: {}", key))),
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
