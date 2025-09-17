use std::io;
use std::process::ExitCode;

use rs_parquet_row_group_stats::filename2stats2stdout;

fn env2parquet_filename() -> Result<String, io::Error> {
    std::env::var("ENV_INPUT_PARQUET_FILENAME")
        .map_err(|e| {
            format!("the input parquet filename ENV_INPUT_PARQUET_FILENAME unspecified: {e}")
        })
        .map_err(io::Error::other)
}

async fn sub() -> Result<(), io::Error> {
    let parquet_filename: String = env2parquet_filename()?;
    filename2stats2stdout(parquet_filename).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
