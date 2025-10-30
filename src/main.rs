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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use visitdir::VisitDir;

const BATCH_SIZE_DEFAULT: usize = 32;
static BATCH_SIZE: AtomicUsize = AtomicUsize::new(BATCH_SIZE_DEFAULT);
static CAT_VARSION: AtomicUsize = AtomicUsize::new(2);

fn set_loglevel(loglevel: &str) {
    std::env::set_var("RUST_LOG", loglevel);
}

fn print_usage(program: &str, opts: &getopts::Options) {
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

    opts.optopt("b", "batch-size", &format!("(Default {BATCH_SIZE_DEFAULT}) Maximum number of files that can be concatenated simultaneously. In other words, with -b 2, the command `cat file.log.FRAG-00001 file.log.FRAG-00002` will be executed."), "");
    opts.optflag("h", "help", "Print this message.");
    opts.optopt("", "log", "One of error, warn, info, debug, trace.", "");
    opts.optflag("v", "verbose", "Same as --log debug.");
    opts.optflag("", "v1", "Use cat version 1 (Default 2).");
    opts.optflag("", "v2", "Use cat version 2 (Default 2).");
    opts.optflag("", "v3", "Use cat version 3 (Default 2).");

    let matches = opts.parse(&args[1..])?;

    if matches.opt_present("h") {
        print_usage(&program, &opts);
        unreachable!();
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
        let number_arg = matches
            .opt_str("batch-size")
            .unwrap_or(format!("{}", BATCH_SIZE_DEFAULT));
        let batch_size: usize = number_arg.parse()?;
        if !(2..=1000).contains(&batch_size) {
            return Err("Invalid batch size.".into());
        }
        assert!(batch_size >= 2);
        BATCH_SIZE.store(batch_size, Ordering::Release);
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
fn catv2(files: &Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs::{remove_file, File, OpenOptions};
    use std::io::{self, Seek, SeekFrom, Write};
    use std::thread::sleep;
    use std::time::Duration;

    fn open_with_retry(path: &str, options: &OpenOptions) -> io::Result<File> {
        loop {
            match options.open(path) {
                Ok(f) => return Ok(f),
                Err(e) => {
                    // 簡単なバックオフ（固定）
                    sleep(Duration::from_millis(100));
                    // ループして再試行（無限リトライ）
                    let _ = e; // エラーを破棄（必要ならログへ出力）
                }
            }
        }
    }

    if files.is_empty() {
        return Ok(());
    }

    // file1 を開く（追記モード）。存在しなければ作成。
    let mut opts = OpenOptions::new();
    opts.write(true).create(true).append(true);
    let mut file1 = open_with_retry(&files[0], &opts)?;

    // file2.. を順に開いて内容を file1 にコピーし、その後削除
    for src_path in files.iter().skip(1) {
        // 読み取りモードで開く（無限リトライ）
        let mut ropts = OpenOptions::new();
        ropts.read(true);
        let mut src = open_with_retry(src_path, &ropts)?;

        // copy を行うために、src の先頭へシーク（念のため）
        let _ = src.seek(SeekFrom::Start(0));

        // std::io::copy を使用して src -> file1
        // 注意: append モードの file1 は内部的に末尾へ書き込まれるが、
        // 複数ファイルを順にコピーするので問題ない。
        io::copy(&mut src, &mut file1)?;

        // コピー完了後、src ファイルを閉じて削除
        drop(src);
        // 削除にもリトライをかける（簡易実装）
        loop {
            match remove_file(src_path) {
                Ok(_) => break,
                Err(_) => {
                    sleep(Duration::from_millis(100));
                }
            }
        }
    }

    // 変更をフラッシュ
    file1.flush()?;

    Ok(())
}

/// Append the content of file2 to file1.
/// file1 will be modified.
/// file2.. will be removed.
/// Returns String object of file1.
/// If opening a file fails, sleep a while and retries infinitely.
pub async fn catv3_async(files: &Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    use std::io;
    use std::path::Path;
    use std::time::Duration;
    use tokio::fs::{remove_file, File, OpenOptions};
    use tokio::io::{self as tokio_io, AsyncSeekExt, AsyncWriteExt};
    use tokio::time::sleep;

    if files.is_empty() {
        return Ok(());
    }

    async fn open_with_retry(path: &Path, opts: &OpenOptions) -> io::Result<File> {
        loop {
            match opts.open(path).await {
                Ok(f) => return Ok(f),
                Err(_e) => {
                    // 固定短時間スリープ後に再試行（無限リトライ）
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    // file1 を開く（追記モード）。存在しなければ作成。
    let mut wopts = OpenOptions::new();
    wopts.write(true).create(true).append(true);
    let mut file1 = open_with_retry(Path::new(&files[0]), &wopts).await?;

    // files[1..] を順に処理
    for src_path in files.iter().skip(1) {
        let src_path_p = Path::new(src_path);

        // 読み取りモードで開く（無限リトライ）
        let mut ropts = OpenOptions::new();
        ropts.read(true);
        let mut src = open_with_retry(src_path_p, &ropts).await?;

        // 先頭にシーク（念のため）
        let _ = src.seek(std::io::SeekFrom::Start(0)).await?;

        // 非同期で copy 相当を実装
        // tokio::io::copy を使うと File->File の copy が可能（AsyncRead + AsyncWrite）
        // ただし file1 は append モードで開いているため末尾に書き込まれる。
        tokio_io::copy(&mut src, &mut file1).await?;

        // 明示的にフラッシュしておく
        file1.flush().await?;

        // src をクローズ（スコープから外す）
        drop(src);

        // 削除にリトライ
        loop {
            match remove_file(src_path_p).await {
                Ok(_) => break,
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
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
fn find_all_files_to_reconstruct2(
) -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error>> {
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
            // If chunk_size is larger than NUM_CAT_ONCE, reconstruct_async() goes infinite loop.
            let chunk_size = std::cmp::min(batch_size, 8);
            for chunk in fragment_filenames.chunks(chunk_size) {
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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = std::time::Instant::now();

    parse_args()?;
    env_logger::init();

    log::debug!("batch_size = {}", BATCH_SIZE.load(Ordering::Acquire));
    log::debug!("Visiting child dir and finding all files to reconstruct.");
    let mut map = find_all_files_to_reconstruct2()?;

    // Check that there are no missing numbers.
    {
        let mut files_to_skip: Vec<String> = Vec::new();
        for (key, val) in map.iter_mut() {
            val.sort_unstable();
            let mut skip_this_file = false;

            for (index, filename) in val.iter().enumerate() {
                let Some(file_num) = filename.split(".FRAG-").last() else {
                    panic!("(BUG) File {filename} does not contain file number.");
                };
                let number = file_num.parse::<usize>().unwrap_or_default();

                if number != index {
                    log::warn!("File {key}.FRAG-{index} is missing. Skip reconstructing {key}.");
                    skip_this_file = true;
                    files_to_skip.push(key.clone());
                    break;
                }
            }

            if skip_this_file {
                val.clear();
            }
        }
        for key in files_to_skip {
            map.remove(&key);
        }
    }

    let mut joinhandles = Vec::new();

    for (key, val) in map.iter() {
        let fragment_filenames = val.clone();
        let handle = tokio::spawn(async move { reconstruct_async(fragment_filenames).await });
        log::info!("Spawned thread for reconstruct {key}");
        joinhandles.push(handle);
    }

    for handle in joinhandles {
        match handle.await {
            Ok(filename) => {
                let new_filename = filename.split(".FRAG-").next().unwrap().to_string();
                log::info!("Filename {new_filename} reconstruction done.");
                std::fs::rename(filename, new_filename).unwrap();
            }
            Err(e) => {
                eprintln!("Error {e:?}");
            }
        }
    }

    log::info!(
        "Reconstruction completed. Elapsed {} ms",
        start_time.elapsed().as_millis()
    );
    Ok(())
}
