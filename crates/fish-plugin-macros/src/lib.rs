use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, Attribute, DeriveInput, Ident, LitStr, Token,
};

/// The `#[derive(Plugin)]` macro — generates a `Plugin` trait implementation
/// from helper attributes on the struct.
///
/// # Usage
///
/// ```ignore
/// use fish_plugin_sdk::prelude::*;
///
/// #[derive(Plugin)]
/// #[plugin(id = "my_plugin", name = "My Plugin")]
/// #[command_handler(id = "ping", pattern = "/ping", func = ping_handler)]
/// #[command_handler(id = "admin", pattern = "/admin", kind = "prefix", func = admin_handler)]
/// #[event_handler(id = "notify", event_type = "order_create", func = on_order)]
/// struct MyPlugin;
///
/// async fn ping_handler(cx: HandlerContext) -> Result<()> { Ok(()) }
/// async fn admin_handler(cx: HandlerContext) -> Result<()> { Ok(()) }
/// async fn on_order(event: Arc<SystemEvent>, adapter: Arc<dyn BaseAdapter>, ctx: Arc<Ctx>) -> Result<()> { Ok(()) }
/// ```
#[proc_macro_derive(Plugin, attributes(plugin, command_handler, event_handler))]
pub fn derive_plugin(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    // Extract #[plugin(...)] attribute
    let plugin_meta = find_attr(&input.attrs, "plugin")
        .and_then(|attr| attr.parse_args::<PluginMeta>().ok())
        .unwrap_or_default();

    // Extract #[command_handler(...)] attributes
    let command_attrs: Vec<CommandAttr> = find_attrs(&input.attrs, "command_handler")
        .filter_map(|attr| attr.parse_args::<CommandAttr>().ok())
        .collect();

    // Extract #[event_handler(...)] attributes
    let event_attrs: Vec<EventAttr> = find_attrs(&input.attrs, "event_handler")
        .filter_map(|attr| attr.parse_args::<EventAttr>().ok())
        .collect();

    // Build metadata
    let id = plugin_meta.id;
    let name = plugin_meta.name;
    let version = plugin_meta.version;
    let description = plugin_meta.description;
    let author = plugin_meta.author;

    // Build message handlers
    let handler_exprs: Vec<_> = command_attrs.iter().map(|cmd| {
        let hid = &cmd.id;
        let pattern = &cmd.pattern;
        let func = &cmd.func;
        match &cmd.kind {
            CommandKind::Exact => {
                quote! {
                    fish_plugin::plugin::MessageHandler::exact(
                        #hid,
                        vec![#pattern],
                        std::sync::Arc::new(move |cx: fish_plugin::plugin::HandlerContext| {
                            Box::pin(#func(cx))
                        }),
                    )
                }
            }
            CommandKind::Prefix => {
                quote! {
                    fish_plugin::plugin::MessageHandler::prefix(
                        #hid,
                        vec![#pattern],
                        std::sync::Arc::new(move |cx: fish_plugin::plugin::HandlerContext| {
                            Box::pin(#func(cx))
                        }),
                    )
                }
            }
            CommandKind::Keyword => {
                quote! {
                    fish_plugin::plugin::MessageHandler::keyword(
                        #hid,
                        vec![#pattern],
                        std::sync::Arc::new(move |cx: fish_plugin::plugin::HandlerContext| {
                            Box::pin(#func(cx))
                        }),
                    )
                }
            }
            CommandKind::Regex => {
                quote! {
                    fish_plugin::plugin::MessageHandler::regex(
                        #hid,
                        #pattern,
                        std::sync::Arc::new(move |cx: fish_plugin::plugin::HandlerContext| {
                            Box::pin(#func(cx))
                        }),
                    )
                }
            }
            CommandKind::Fallback => {
                quote! {
                    fish_plugin::plugin::MessageHandler::fallback(
                        #hid,
                        std::sync::Arc::new(move |cx: fish_plugin::plugin::HandlerContext| {
                            Box::pin(#func(cx))
                        }),
                    )
                }
            }
        }
    }).collect();

    // Build event handlers
    let event_exprs: Vec<_> = event_attrs.iter().map(|evt| {
        let et = &evt.event_type;
        let eid = &evt.id;
        let func = &evt.func;
        quote! {
            map.insert(
                String::from(#et),
                vec![fish_plugin::plugin::EventHandler::new(
                    #eid,
                    std::sync::Arc::new(move |event, adapter, ctx| Box::pin(#func(event, adapter, ctx))),
                )],
            );
        }
    }).collect();

    let expanded = quote! {
        impl fish_plugin::plugin::Plugin for #struct_name {
            fn metadata(&self) -> &fish_plugin::plugin::PluginMetadata {
                static META: std::sync::LazyLock<fish_plugin::plugin::PluginMetadata> =
                    std::sync::LazyLock::new(|| {
                        fish_plugin::plugin::PluginMetadata {
                            id: String::from(#id),
                            name: String::from(#name),
                            description: String::from(#description),
                            version: String::from(#version),
                            author: String::from(#author),
                        }
                    });
                &META
            }

            fn message_handlers(&self) -> &[fish_plugin::plugin::MessageHandler] {
                static HANDLERS: std::sync::LazyLock<Vec<fish_plugin::plugin::MessageHandler>> =
                    std::sync::LazyLock::new(|| {
                        vec![
                            #(#handler_exprs),*
                        ]
                    });
                &HANDLERS
            }

            fn event_handlers(&self) -> std::collections::HashMap<String, Vec<fish_plugin::plugin::EventHandler>> {
                let mut map = std::collections::HashMap::new();
                #(#event_exprs)*
                map
            }
        }
    };

    TokenStream::from(expanded)
}

// ---- Attribute parsing types ----

/// `#[plugin(id = "...", name = "...", ...)]`
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

/// Command kind
enum CommandKind {
    Exact,
    Prefix,
    Keyword,
    Regex,
    Fallback,
}

impl Default for CommandKind {
    fn default() -> Self {
        CommandKind::Exact
    }
}

impl CommandKind {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "exact" => Some(CommandKind::Exact),
            "prefix" => Some(CommandKind::Prefix),
            "keyword" => Some(CommandKind::Keyword),
            "regex" => Some(CommandKind::Regex),
            "fallback" => Some(CommandKind::Fallback),
            _ => None,
        }
    }
}

/// `#[command_handler(id = "...", pattern = "...", kind = "exact", func = function_name)]`
struct CommandAttr {
    id: String,
    pattern: String,
    kind: CommandKind,
    func: Ident,
}

impl Parse for CommandAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut id = None;
        let mut pattern = None;
        let mut kind = CommandKind::default();
        let mut func = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            match key.to_string().as_str() {
                "id" => {
                    let value: LitStr = input.parse()?;
                    id = Some(value.value());
                }
                "pattern" => {
                    let value: LitStr = input.parse()?;
                    pattern = Some(value.value());
                }
                "kind" => {
                    let value: LitStr = input.parse()?;
                    kind = CommandKind::from_str(&value.value())
                        .ok_or_else(|| syn::Error::new(value.span(), "unknown command kind: expected exact, prefix, keyword, regex, or fallback"))?;
                }
                "func" => {
                    func = Some(input.parse()?);
                }
                _ => return Err(syn::Error::new(key.span(), format!("unknown command_handler key: {}", key))),
            }
            if !input.is_empty() {
                let _: Token![,] = input.parse()?;
            }
        }

        let id = id.ok_or_else(|| syn::Error::new(input.span(), "command_handler id is required"))?;
        let pattern = pattern.ok_or_else(|| syn::Error::new(input.span(), "command_handler pattern is required"))?;
        let func = func.ok_or_else(|| syn::Error::new(input.span(), "command_handler func is required"))?;

        Ok(CommandAttr { id, pattern, kind, func })
    }
}

/// `#[event_handler(id = "...", event_type = "...", func = function_name)]`
struct EventAttr {
    id: String,
    event_type: String,
    func: Ident,
}

impl Parse for EventAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut id = None;
        let mut event_type = None;
        let mut func = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            match key.to_string().as_str() {
                "id" => {
                    let value: LitStr = input.parse()?;
                    id = Some(value.value());
                }
                "event_type" => {
                    let value: LitStr = input.parse()?;
                    event_type = Some(value.value());
                }
                "func" => {
                    func = Some(input.parse()?);
                }
                _ => return Err(syn::Error::new(key.span(), format!("unknown event_handler key: {}", key))),
            }
            if !input.is_empty() {
                let _: Token![,] = input.parse()?;
            }
        }

        let id = id.ok_or_else(|| syn::Error::new(input.span(), "event_handler id is required"))?;
        let event_type = event_type.ok_or_else(|| syn::Error::new(input.span(), "event_handler event_type is required"))?;
        let func = func.ok_or_else(|| syn::Error::new(input.span(), "event_handler func is required"))?;

        Ok(EventAttr { id, event_type, func })
    }
}

// ---- Helper functions ----

fn find_attr<'a>(attrs: &'a [Attribute], name: &str) -> Option<&'a Attribute> {
    attrs.iter().find(|attr| attr.path().is_ident(name))
}

fn find_attrs<'a>(attrs: &'a [Attribute], name: &str) -> impl Iterator<Item = &'a Attribute> {
    attrs.iter().filter(move |attr| attr.path().is_ident(name))
}
