use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, ItemFn};

pub(crate) fn expand(item: ItemFn) -> Result<TokenStream, Error> {
    // Must NOT be async
    if item.sig.asyncness.is_some() {
        return Err(Error::new_spanned(
            item.sig.fn_token,
            "#[pipeline] function must not be async (the macro adds #[tokio::main])",
        ));
    }

    // Must be named "main"
    if item.sig.ident != "main" {
        return Err(Error::new_spanned(
            &item.sig.ident,
            "#[pipeline] function must be named `main`",
        ));
    }

    // Must have exactly 1 param (the graph ref)
    let params: Vec<_> = item.sig.inputs.iter().collect();
    if params.len() != 1 {
        return Err(Error::new_spanned(
            &item.sig.inputs,
            "#[pipeline] function must have exactly 1 parameter: (graph: &DataflowGraph)",
        ));
    }

    let first_param = &params[0];
    let syn::FnArg::Typed(first_pat) = first_param else {
        return Err(Error::new_spanned(
            first_param,
            "#[pipeline] function must not have a self parameter",
        ));
    };
    let graph_pat = &*first_pat.pat;

    let body = &item.block;

    Ok(quote! {
        #[::rhei::__private::tokio::main]
        async fn main() -> ::rhei::__private::anyhow::Result<()> {
            #[derive(::rhei::__private::clap::Parser)]
            #[command(about = "Rhei stream processing pipeline")]
            struct __RheiArgs {
                /// Number of parallel Timely worker threads
                #[arg(long, env = "RHEI_WORKERS", default_value = "1")]
                workers: usize,

                /// Checkpoint directory
                #[arg(long, env = "RHEI_CHECKPOINT_DIR")]
                checkpoint_dir: Option<::std::path::PathBuf>,

                /// Bind address for the HTTP health/metrics server
                #[arg(long, env = "RHEI_METRICS_ADDR")]
                metrics_addr: Option<::std::net::SocketAddr>,

                /// Process ID for multi-process cluster mode (0-based)
                #[arg(long, env = "RHEI_PROCESS_ID")]
                process_id: Option<usize>,

                /// Comma-separated peer addresses for cluster mode
                #[arg(long, env = "RHEI_PEERS", value_delimiter = ',')]
                peers: Option<Vec<String>>,

                /// Resume from a remote checkpoint (manifest path in the
                /// configured remote bucket, e.g. `checkpoints/manifest.json`)
                #[arg(long, env = "RHEI_FROM_CHECKPOINT")]
                from_checkpoint: Option<String>,

                /// Signed offset delta applied to source offsets in fork mode
                #[arg(long, env = "RHEI_OFFSET_DELTA", default_value = "0")]
                offset_delta: i64,

                /// Pipeline display name
                #[arg(long, env = "RHEI_PIPELINE_NAME")]
                pipeline_name: Option<String>,
            }

            let __args = <__RheiArgs as ::rhei::__private::clap::Parser>::parse();

            let __rhei_dir = __args.checkpoint_dir
                .unwrap_or_else(|| ::std::env::temp_dir().join("rhei_pipeline"));
            ::std::fs::create_dir_all(&__rhei_dir)?;

            let __rhei_graph = ::rhei::DataflowGraph::new();
            {
                let #graph_pat = &__rhei_graph;
                #body
            }

            let mut __builder = ::rhei::PipelineController::builder()
                .checkpoint_dir(&__rhei_dir)
                .workers(__args.workers)
                .offset_delta(__args.offset_delta);

            #[cfg(feature = "remote-state")]
            if let Some(ref url) = __args.from_checkpoint {
                __builder = __builder.from_checkpoint(url);
            }
            #[cfg(not(feature = "remote-state"))]
            if __args.from_checkpoint.is_some() {
                ::rhei::__private::anyhow::bail!(
                    "--from-checkpoint requires the `remote-state` feature"
                );
            }

            if let Some(addr) = __args.metrics_addr {
                __builder = __builder.metrics_addr(addr);
            }
            if let Some(pid) = __args.process_id {
                __builder = __builder.process_id(pid);
            }
            if let Some(ref peers) = __args.peers {
                __builder = __builder.peers(peers.clone());
            }
            if let Some(ref name) = __args.pipeline_name {
                __builder = __builder.pipeline_name(name);
            }

            // Pick up remote state config from env (not in clap args
            // because it has 5 sub-fields better handled by from_env).
            __builder = __builder.from_env();

            __builder.build()?.start(__rhei_graph).await?;
            Ok(())
        }
    })
}
