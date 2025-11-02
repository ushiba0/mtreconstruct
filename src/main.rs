extern crate env_logger;
extern crate getopts;
extern crate log;

mod visitdir;

use regex::Regex;
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use visitdir::VisitDir;

const BATCH_SIZE_DEFAULT: usize = 100000;
static BATCH_SIZE: AtomicUsize = AtomicUsize::new(BATCH_SIZE_DEFAULT);
static CAT_VARSION: AtomicUsize = AtomicUsize::new(2);
static FORCE_RECONSTRUCT: AtomicBool = AtomicBool::new(false);

fn set_loglevel(loglevel: &str) {
    std::env::set_var("RUST_LOG", loglevel);
}

fn print_usage(program: &str, opts: &getopts::Options) -> ! {
    let brief = format!(
        "Multithread reconstruction.
Usage: {program}
       {program} -n [NUMBER]"
    );
    print!("{}", opts.usage(&brief));
    std::process::exit(0);
}

fn parse_args() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = getopts::Options::new();

    opts.optflag("h", "help", "Print this message.");
    opts.optopt("", "log", "One of error, warn, info, debug, trace.", "");
    opts.optflag("v", "verbose", "Same as --log debug.");
    opts.optflag("1", "v1", "Use cat version 1 (Default 2).");
    opts.optflag("2", "v2", "Use cat version 2 (Default 2).");
    opts.optflag("3", "v3", "Use cat version 3 (Default 2).");
    opts.optopt("b", "batch-size", &format!("(Default {BATCH_SIZE_DEFAULT}) Maximum number of files that can be concatenated simultaneously. In other words, with -b 2, the command `cat file.log.FRAG-00001 file.log.FRAG-00002` will be executed."), "");
    opts.optflag("f", "force", "The file suffixes are expected to be .FRAG-00000, .FRAG-00001, .FRAG-00002, and so on. By default, if the files do not match this pattern, reconstruction is skipped. The --force option bypasses this verification and forcibly concatenates the files.");

    let matches = opts.parse(&args[1..])?;

    if matches.opt_present("h") {
        print_usage(&program, &opts);
    }

    if matches.opt_present("log") {
        let loglevel = matches.opt_str("log").unwrap_or_else(|| "info".to_string());
        set_loglevel(&loglevel);
    }

    if matches.opt_present("v") {
        set_loglevel("debug");
    }

    if matches.opt_present("v2") {
        CAT_VARSION.store(2, Ordering::Release);
    } else if matches.opt_present("v3") {
        CAT_VARSION.store(3, Ordering::Release);
    }

    if matches.opt_present("batch-size") {
        let number_arg = matches.opt_str("batch-size").unwrap_or(format!("{}", BATCH_SIZE_DEFAULT));
        let batch_size: usize = number_arg.parse()?;
        if !(2..).contains(&batch_size) {
            return Err("Invalid batch size.".into());
        }
        assert!(batch_size >= 2);
        BATCH_SIZE.store(batch_size, Ordering::Release);
    }

    if matches.opt_present("f") {
        FORCE_RECONSTRUCT.store(true, Ordering::Release);
    }

    Ok(())
}

/// Append the content of file2 to file1.
/// file1 will be modified.
/// file2.. will be removed.
/// Returns String object of file1.
/// If opening a file fails, sleep a while and retries infinitely.
fn catv1(files: &Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    log::trace!("Reconstructing {files:?}");
    if files.len() <= 1 {
        return Ok(());
    }
    for file in files.iter() {
        if file.is_empty() {
            panic!("(BUG) Filename is empty.");
        }
    }

    let f1 = std::fs::OpenOptions::new().append(true).open(&files[0])?;
    let mut buf1 = std::io::BufWriter::new(f1);

    for file in files.iter().skip(1) {
        if file.is_empty() {
            continue;
        }

        let f2 = std::fs::File::open(file)?;
        let mut buf2 = std::io::BufReader::new(f2);

        let mut b: Vec<u8> = Vec::new();
        buf2.read_to_end(&mut b)?;
        buf1.write_all(&b)?;
        std::fs::remove_file(file)?;
    }

    Ok(())
}

/// Append the content of file2 to file1.
/// file1 will be modified.
/// file2.. will be removed.
/// Returns String object of file1.
/// If opening a file fails, sleep a while and retries infinitely.
fn catv2(files: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Seek, SeekFrom};

    fn open_with_retry(path: &str, retry: usize, dur_ms: u64, opts: &std::fs::OpenOptions) -> std::fs::File {
        for _ in 0..retry {
            match opts.open(path) {
                Ok(f) => return f,
                Err(e) => {
                    log::warn!("File {path} open failed. {e:?} Retry in {dur_ms} ms.");
                    let duration = std::time::Duration::from_millis(dur_ms);
                    std::thread::sleep(duration);
                }
            }
        }
        panic!("File {path} open failed after {retry} retries.");
    }

    fn delete_with_retry(path: &str, retry: usize, dur_ms: u64) {
        for _ in 0..retry {
            match std::fs::remove_file(path) {
                Ok(_) => return,
                Err(e) => {
                    log::warn!("File {path} remove failed. {e:?} Retry in {dur_ms} ms.");
                    let duration = std::time::Duration::from_millis(dur_ms);
                    std::thread::sleep(duration);
                }
            }
        }
        panic!("File {path} remove failed after {retry} retries.");
    }

    if files.len() <= 1 {
        return Ok(());
    }

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(false).append(true);
    let mut file1 = open_with_retry(&files[0], 10, 1000, &opts);

    // Open files[1], files[2], ... and append them to files[0].
    for src_path in files.iter().skip(1) {
        // Open file.
        let mut ropts = std::fs::OpenOptions::new();
        ropts.read(true);
        let mut src = open_with_retry(src_path, 10, 1000, &ropts);

        // Seek to start (念のため).
        let _ = src.seek(SeekFrom::Start(0));

        // Append src file to file1.
        std::io::copy(&mut src, &mut file1)?;

        // Remove src file.
        drop(src);
        delete_with_retry(&src_path, 10, 1000);
    }

    file1.flush()?;

    Ok(())
}

/// Append the content of file2 to file1.
/// file1 will be modified.
/// file2.. will be removed.
/// Returns String object of file1.
/// If opening a file fails, sleep a while and retries infinitely.
pub async fn catv3_async(files: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    if files.len() <= 1 {
        return Ok(());
    }

    async fn open_with_retry(path: &str, retry: usize, dur_ms: u64, opts: &tokio::fs::OpenOptions) -> tokio::fs::File {
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

    async fn delete_with_retry(path: &str, retry: usize, dur_ms: u64) {
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

    let mut wopts = tokio::fs::OpenOptions::new();
    wopts.write(true).create(false).append(true);
    let mut file1 = open_with_retry(&files[0], 10, 1000, &wopts).await;

    // Open files[1], files[2], ... and append them to files[0].
    for src_path in files.iter().skip(1) {
        // Open file.
        let mut ropts = tokio::fs::OpenOptions::new();
        ropts.read(true);
        let mut src = open_with_retry(&src_path, 10, 1000, &ropts).await;

        // Seek to start (念のため).
        let _ = src.seek(std::io::SeekFrom::Start(0)).await?;

        // Append src file to file1.
        tokio::io::copy(&mut src, &mut file1).await?;

        // Remove src file.
        drop(src);
        delete_with_retry(&src_path, 10, 1000).await;
    }

    // 最後に file1 をフラッシュして終了
    file1.flush().await?;

    Ok(())
}

/// Find all files to reconstruct.
/// Group a list of file paths by their base filename (prefix before ".FRAG-").
///
/// For example, given "foo.txt.FRAG-001" and "foo.txt.FRAG-002",
/// both will be grouped under the key "foo.txt".
fn find_all_files_to_reconstruct2() -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error>> {
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
            .or_insert_with(|| {
                log::debug!("Found file {file_key}");
                vec![filename]
            });
    }

    Ok(map)
}

// Concatinates files.
// Returns filename.
fn reconstruct_async(fragment_filenames: Vec<String>) -> impl Future<Output = String> + Send {
    log::trace!("[reconstruct_async] {fragment_filenames:?}");
    async move {
        let batch_size = BATCH_SIZE.load(Ordering::Acquire);
        if fragment_filenames.len() <= batch_size {
            // Concatinate!
            match CAT_VARSION.load(Ordering::Acquire) {
                1 => catv1(&fragment_filenames).unwrap(),
                2 => catv2(&fragment_filenames).unwrap(),
                3 => catv3_async(&fragment_filenames).await.unwrap(),
                _ => unreachable!("(BUG)"),
            }

            fragment_filenames[0].clone()
        } else {
            let mut handles = Vec::new();
            for chunk in fragment_filenames.chunks(batch_size) {
                let files = chunk.to_vec();
                let handle = tokio::spawn(async move { reconstruct_async(files).await });
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

            let handle = tokio::spawn(async move { reconstruct_async(files).await });

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
fn verify_file_number(file_map: &mut HashMap<String, Vec<String>>) {
    let mut files_to_skip: Vec<String> = Vec::new();

    for (key, val) in file_map.iter() {
        for (index, filename) in val.iter().enumerate() {
            let Some(file_num) = filename.split(".FRAG-").last() else {
                panic!("(BUG) File {filename} does not contain file number.");
            };
            let number = file_num.parse::<usize>().unwrap_or_default();

            if number != index {
                if FORCE_RECONSTRUCT.load(Ordering::Acquire) {
                    log::warn!("File {key}.FRAG-{index:>05} is missing, but continue reconstruction.");
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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = std::time::Instant::now();

    parse_args()?;
    env_logger::init();

    log::debug!("batch_size = {}", BATCH_SIZE.load(Ordering::Acquire));
    log::debug!("Visiting child dir and finding all files to reconstruct.");
    let mut map = find_all_files_to_reconstruct2()?;

    for (_, val) in map.iter_mut() {
        val.sort_unstable();
    }

    verify_file_number(&mut map);

    let mut joinhandles = Vec::new();

    for (key, val) in map.iter() {
        let fragment_filenames = val.clone();
        let filename = key.clone();
        let handle = tokio::spawn(async move {
            let thread_start_time = std::time::Instant::now();
            let res = reconstruct_async(fragment_filenames).await;
            let elapsed = thread_start_time.elapsed().as_millis();
            log::info!("Reconstruction of {filename} completed. Elapsed {elapsed} ms.");

            // Rename file.
            match tokio::fs::rename(&res, &filename).await {
                Ok(_) => {}
                Err(e) => {
                    log::warn!("Failed to rename {res} to {filename}. {e:?}")
                }
            }
            res
        });
        log::info!("Spawned thread for reconstruct {key}");
        joinhandles.push(handle);
    }

    for handle in joinhandles {
        match handle.await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error {e:?}");
            }
        }
    }

    log::info!("Reconstruction completed. Elapsed {} ms", start_time.elapsed().as_millis());
    Ok(())
}
