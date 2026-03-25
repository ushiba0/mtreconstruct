mod visitdir;

use std::collections::HashMap;
use std::future::Future;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use visitdir::VisitDir;

use anyhow::anyhow;
use clap::Parser;
use regex::Regex;
use tokio::task::JoinHandle;

// Constants and command line options.
const BATCH_SIZE_DEFAULT: usize = 100000;
static BATCH_SIZE: AtomicUsize = AtomicUsize::new(BATCH_SIZE_DEFAULT);
static CAT_ASYNC: AtomicBool = AtomicBool::new(false);
static FORCE_RECONSTRUCT: AtomicBool = AtomicBool::new(false);
static DRY_RUN: AtomicBool = AtomicBool::new(false);

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    help_template = "\
{before-help}{name} {version}
{author-with-newline}{about-with-newline}
GitHub: https://github.com/ushiba0/mtreconstruct
{usage-heading} {usage}

{all-args}{after-help}"
)]
struct Args {
    /// One of error, warn, info, debug, trace.
    #[arg(long, value_name = "LEVEL")]
    log: Option<String>,

    /// Same as --log debug.
    #[arg(short, long)]
    verbose: bool,

    /// Dry run mode.
    #[arg(long)]
    dry_run: bool,

    /// Use tokio::io::copy() instead of std::io::copy(). (May be slower than default.)
    #[arg(short = 'a', long = "async")]
    runasync: bool,

    /// Maximum number of files that can be concatenated simultaneously.
    #[arg(short, long, default_value_t = BATCH_SIZE_DEFAULT as u64,  value_parser = clap::value_parser!(u64).range(2..))]
    batch_size: u64,

    /// Forcibly concatenates files even if fragment numbers are not consecutive.
    #[arg(short, long)]
    force: bool,
}

fn init_logger(loglevel: &str) -> anyhow::Result<()> {
    use env_logger::Builder;
    use log::LevelFilter;

    let mut builder = Builder::from_default_env();
    let level = match loglevel.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => return Err(anyhow!("Invalid log level: {}", loglevel)),
    };

    builder.filter_level(level).init();
    Ok(())
}

fn parse_args() -> anyhow::Result<Arc<Args>> {
    let args = Args::parse();

    let loglevel = if let Some(loglevel) = args.log.as_ref() {
        loglevel.clone()
    } else if args.verbose {
        "debug".to_string()
    } else {
        "warn".to_string()
    };
    init_logger(&loglevel)?;

    CAT_ASYNC.store(args.runasync, Ordering::Release);
    DRY_RUN.store(args.dry_run, Ordering::Release);
    BATCH_SIZE.store(args.batch_size as usize, Ordering::Release);
    FORCE_RECONSTRUCT.store(args.force, Ordering::Release);

    Ok(Arc::new(args))
}

async fn delete_with_retry_async(path: &str, retry: usize, dur_ms: u64) {
    for _ in 0..retry {
        match tokio::fs::remove_file(path).await {
            Ok(_) => return,
            Err(e) => {
                log::warn!("File {path} remove failed. {e:?} Retry in {dur_ms} ms.");
                let duration = tokio::time::Duration::from_millis(dur_ms);
                tokio::time::sleep(duration).await;
            }
        }
    }
    panic!("File {path} remove failed after {retry} retries.");
}

async fn open_with_retry_async(path: &str, retry: usize, dur_ms: u64, opts: &tokio::fs::OpenOptions) -> tokio::fs::File {
    for _ in 0..retry {
        match opts.open(path).await {
            Ok(f) => return f,
            Err(e) => {
                log::warn!("File {path} open failed. {e:?} Retry in {dur_ms} ms.");
                let duration = tokio::time::Duration::from_millis(dur_ms);
                tokio::time::sleep(duration).await;
            }
        }
    }
    panic!("File {path} open failed after {retry} retries.");
}

/// Append the content of file2, file3, ... to file1.
/// file1 will be modified.
/// file2, file3, ... will be removed.
/// Returns String filename of file1.
/// If opening a file fails, sleep a while and retries infinitely.
async fn concatinate(files: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Seek, SeekFrom};

    if files.len() <= 1 {
        return Ok(());
    }

    let mut wopts = tokio::fs::OpenOptions::new();
    wopts.write(true).create(false).append(true);
    let mut file1 = open_with_retry_async(&files[0], 10, 1000, &wopts).await.into_std().await;

    // Open files[1], files[2], ... and append them to files[0].
    for src_path in files.iter().skip(1) {
        // Open file.
        let mut ropts = tokio::fs::OpenOptions::new();
        ropts.read(true);
        let mut src = open_with_retry_async(src_path, 10, 1000, &ropts).await.into_std().await;

        // Seek to start (念のため).
        let _ = src.seek(SeekFrom::Start(0));

        // Append src file to file1.
        std::io::copy(&mut src, &mut file1)?;

        // Remove src file.
        drop(src);
        delete_with_retry_async(src_path, 10, 1000).await;
    }

    file1.flush()?;

    Ok(())
}

/// Append the content of file2, file3, ... to file1.
/// file1 will be modified.
/// file2, file3, ... will be removed.
/// Returns String filename of file1.
/// If opening a file fails, sleep a while and retries infinitely.
pub async fn concatinate_async(files: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    if files.len() <= 1 {
        return Ok(());
    }

    let mut wopts = tokio::fs::OpenOptions::new();
    wopts.write(true).create(false).append(true);
    let mut file1 = open_with_retry_async(&files[0], 10, 1000, &wopts).await;

    // Open files[1], files[2], ... and append them to files[0].
    for src_path in files.iter().skip(1) {
        // Open file.
        let mut ropts = tokio::fs::OpenOptions::new();
        ropts.read(true);
        let mut src = open_with_retry_async(src_path, 10, 1000, &ropts).await;

        // Seek to start (念のため).
        let _ = src.seek(std::io::SeekFrom::Start(0)).await?;

        // Append src file to file1.
        tokio::io::copy(&mut src, &mut file1).await?;

        // Remove src file.
        drop(src);
        delete_with_retry_async(src_path, 10, 1000).await;
    }

    file1.flush().await?;

    Ok(())
}

/// Find all files to reconstruct.
/// Group a list of file paths by their base filename (prefix before ".FRAG-").
///
/// For example, given "foo.txt.FRAG-001" and "foo.txt.FRAG-002",
/// both will be grouped under the key "foo.txt".
fn find_all_files_to_reconstruct() -> anyhow::Result<HashMap<String, Vec<String>>> {
    let re = Regex::new(r".FRAG-")?;
    let file_iter = VisitDir::new(".")?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();

    for entry in file_iter {
        let filename = entry?.path().to_string_lossy().into_owned();
        if !re.is_match(&filename) {
            continue;
        }

        let file_key = filename.split(".FRAG-").next().unwrap().to_string();

        map.entry(file_key.clone())
            .and_modify(|files| files.push(filename.clone()))
            .or_insert_with(|| vec![filename]);
    }

    Ok(map)
}

// Concatinates files.
// Returns filename.
fn reconstruct_async(fragment_filenames: Vec<String>, args: Arc<Args>) -> impl Future<Output = String> + Send {
    log::trace!("[reconstruct_async] {fragment_filenames:?}");
    async move {
        if fragment_filenames.len() <= args.batch_size as usize {
            // Concatinate!
            let res = if CAT_ASYNC.load(Ordering::Acquire) {
                concatinate_async(&fragment_filenames).await
            } else {
                concatinate(&fragment_filenames).await
            };
            match res {
                Ok(_) => {}
                Err(e) => {
                    log::error!("Error while concatinate files {}.. {e:?}", fragment_filenames[0]);
                    panic!();
                }
            }
            fragment_filenames[0].clone()
        } else {
            let mut handles = Vec::new();
            for chunk in fragment_filenames.chunks(args.batch_size as usize) {
                let files = chunk.to_vec();
                let args1 = args.clone();
                let handle = tokio::spawn(async move { reconstruct_async(files, args1).await });
                handles.push(handle);
            }

            let mut files = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok(filename) => files.push(filename),
                    Err(e) => {
                        log::error!("Error {e:?}");
                        panic!();
                    }
                }
            }

            let args1: Arc<Args> = args.clone();
            let handle = tokio::spawn(async move { reconstruct_async(files, args1).await });

            match handle.await {
                Ok(filename) => filename,
                Err(e) => {
                    log::error!("Error {e:?}");
                    panic!();
                }
            }
        }
    }
}

/// Check whether the fragment numbers are consecutive.
/// Example:
///     If .FRAG-00001 is missing, as in .FRAG-00000, .FRAG-00002, .FRAG-00003, ..., remove the key from file_map.
fn verify_fragment_number(file_map: &mut HashMap<String, Vec<String>>) {
    let mut files_to_skip: Vec<String> = Vec::new();

    for (key, val) in file_map.iter() {
        for (index, filename) in val.iter().enumerate() {
            let Some(file_num) = filename.split(".FRAG-").last() else {
                panic!("(BUG) File {filename} does not contain file number.");
            };
            let number = file_num.parse::<usize>().unwrap_or_default();

            if number != index {
                if FORCE_RECONSTRUCT.load(Ordering::Acquire) {
                    log::warn!("File {key}.FRAG-{index:>05} is missing, but continuing reconstruction.");
                    break;
                } else {
                    log::warn!("File {key}.FRAG-{index:>05} is missing. Skip reconstruction of {key}.");
                    log::warn!("[HINT] Try --force option.");
                    files_to_skip.push(key.clone());
                    break;
                }
            }
        }
    }
    for key in files_to_skip {
        file_map.remove(&key);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    let start_time = std::time::Instant::now();
    let args = parse_args()?;

    log::debug!("batch_size = {}", BATCH_SIZE.load(Ordering::Acquire));
    if args.runasync {
        log::info!("Using tokio::io::copy().");
    }
    log::debug!("Finding all files to reconstruct.");
    let mut map = find_all_files_to_reconstruct()?;

    for (_, val) in map.iter_mut() {
        val.sort_unstable();
    }

    verify_fragment_number(&mut map);

    let mut joinhandles: Vec<JoinHandle<anyhow::Result<()>>> = Vec::new();

    for (key, val) in map.iter() {
        let fragment_filenames = val.clone();
        let filename = key.clone();
        let args1 = args.clone();
        let handle = tokio::spawn(async move {
            log::debug!("Thread for reconstruct {filename} start working.");
            if args1.dry_run {
                return Ok(());
            }
            let thread_start_time = std::time::Instant::now();
            let file_num = fragment_filenames.len();
            let filename_reconstructed = reconstruct_async(fragment_filenames, args1).await;
            let elapsed = thread_start_time.elapsed().as_millis();
            let meta = tokio::fs::metadata(&filename_reconstructed)
                .await
                .map_err(|e| anyhow!("Failed to get metadata of {filename_reconstructed}: {e}"))?;
            let size_mb = meta.len() / 1024 / 1024;
            log::info!(
                "Reconstruction of {filename} completed. Total files = {file_num}, Size = {size_mb} MiB, Elapsed = {elapsed} ms."
            );

            // Rename file.
            tokio::fs::rename(&filename_reconstructed, &filename)
                .await
                .map_err(|e| anyhow!("Failed to rename {filename}: {e}"))?;
            Ok(())
        });
        log::info!("Spawned tokio thread for reconstruct {key}");
        joinhandles.push(handle);
    }

    for handle in joinhandles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                log::error!("Task error: {e}");
            }
            Err(e) => {
                log::error!("JoinError: {e}");
            }
        }
    }

    log::info!("Reconstruction completed. Elapsed {} ms", start_time.elapsed().as_millis());
    Ok(())
}
