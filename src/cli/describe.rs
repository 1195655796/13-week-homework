use super::ReplResult;
use crate::{Backend, CmdExector, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(help = "The name of the dataset")]
    pub name: String,
}

pub fn describe(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let (msg, rx) = ReplMsg::new(DescribeOpts::new(name));
    Ok(ctx.send(msg, rx))
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl CmdExector for DescribeOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.describe(&self.name).await?;
        df.display().await
    }
}
