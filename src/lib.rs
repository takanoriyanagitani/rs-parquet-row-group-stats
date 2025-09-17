use std::io;
use std::path::Path;
use std::pin::Pin;

use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use futures::stream::Stream;
use futures::stream::TryStreamExt;

use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::ChunkReader;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(serde::Serialize)]
pub struct BasicRowGroupStats {
    pub num_columns: usize,
    pub num_rows: i64,
    pub total_byte_size: i64,
    pub compressed_size: i64,
    pub ordinal: Option<i16>,
    pub file_offset: Option<i64>,
}

impl From<&RowGroupMetaData> for BasicRowGroupStats {
    fn from(m: &RowGroupMetaData) -> Self {
        let num_columns: usize = m.num_columns();
        let num_rows: i64 = m.num_rows();
        let total_byte_size: i64 = m.total_byte_size();
        let compressed_size: i64 = m.compressed_size();

        let ordinal: Option<i16> = m.ordinal();
        let file_offset: Option<i64> = m.file_offset();

        BasicRowGroupStats {
            num_columns,
            num_rows,
            total_byte_size,
            compressed_size,
            ordinal,
            file_offset,
        }
    }
}

pub fn pmd2rgmd(pmd: &ParquetMetaData) -> &[RowGroupMetaData] {
    pmd.row_groups()
}

pub fn prbrb2rgmd<T>(prbrb: &ParquetRecordBatchReaderBuilder<T>) -> &[RowGroupMetaData]
where
    T: ChunkReader,
{
    let met: &ParquetMetaData = prbrb.metadata();
    pmd2rgmd(met)
}

pub fn file2prbrb(
    file: std::fs::File,
) -> Result<ParquetRecordBatchReaderBuilder<std::fs::File>, io::Error> {
    ParquetRecordBatchReaderBuilder::try_new(file).map_err(io::Error::other)
}

pub fn file2stats(
    file: std::fs::File,
) -> Pin<Box<dyn Stream<Item = Result<BasicRowGroupStats, io::Error>>>> {
    let strm = async_stream::try_stream! {
        let prbrb = file2prbrb(file)?;
        let rgmd: &[RowGroupMetaData] = prbrb2rgmd(&prbrb);
        for item in rgmd {
            let b: BasicRowGroupStats = item.into();
            yield b;
        }
    };
    Box::pin(strm)
}

pub fn stats2buf(s: &BasicRowGroupStats, buf: &mut Vec<u8>) -> Result<(), io::Error> {
    buf.clear();
    serde_json::to_writer(buf, s)?;
    Ok(())
}

pub async fn print_stats<S, W>(mut stats: S, mut w: W) -> Result<(), io::Error>
where
    S: Unpin + Stream<Item = Result<BasicRowGroupStats, io::Error>>,
    W: Unpin + AsyncWrite,
{
    let mut buf: Vec<u8> = vec![];
    while let Some(brgs) = stats.try_next().await? {
        stats2buf(&brgs, &mut buf)?;
        w.write_all(&buf).await?;
    }
    w.flush().await?;
    Ok(())
}

pub async fn file2stats2writer<W>(file: std::fs::File, wtr: W) -> Result<(), io::Error>
where
    W: Unpin + AsyncWrite,
{
    let stats = file2stats(file);
    print_stats(stats, wtr).await
}

pub async fn file2stats2stdout(file: std::fs::File) -> Result<(), io::Error> {
    let wtr = tokio::io::stdout();
    file2stats2writer(file, wtr).await
}

pub async fn filename2stats2stdout<P>(filename: P) -> Result<(), io::Error>
where
    P: AsRef<Path>,
{
    let f = std::fs::File::open(filename)?;
    file2stats2stdout(f).await
}
