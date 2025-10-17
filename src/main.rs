extern crate env_logger;
extern crate getopts;
extern crate log;

mod visitdir;

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::sync::Mutex;

use visitdir::VisitDir;

const NUM_CAT_ONCE_DEFATLT: usize = 32;
static NUM_CAT_ONCE: Lazy<Mutex<usize>> = Lazy::new(|| Mutex::new(NUM_CAT_ONCE_DEFATLT));

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

    opts.optopt("n", "number", "number", "");
    opts.optflag("h", "help", "Print this message.");
    opts.optopt("", "log", "debug, info, warn, error", "");

    if args.iter().any(|e| e == "--test") {
        //test_code();
        unreachable!();
    }

    let matches = opts.parse(&args[1..])?;

    if matches.opt_present("h") {
        print_usage(&program, &opts);
        unreachable!();
    }

    if matches.opt_present("log") {
        let loglevel = matches.opt_str("log").unwrap_or_else(|| "info".to_string());
        set_loglevel(&loglevel);
    }

    if matches.opt_present("number") {
        let number_arg = matches
            .opt_str("number")
            .unwrap_or(format!("{}", NUM_CAT_ONCE_DEFATLT));
        let number: usize = number_arg.parse()?;
        if !(2..=100).contains(&number) {
            let number_error = std::io::Error::new(std::io::ErrorKind::Other, "Input number error");
            return Err(Box::new(number_error));
        }
        assert!(number > 1);
        *NUM_CAT_ONCE.lock()? = number;
    }

    Ok(())
}

// Append the content of file2 to file1.
// file1 will be modified.
// file2.. will be removed.
fn cat(files: &Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    log::trace!("Reconstructing {files:?}");
    if files.len() <= 1 {
        return Ok(());
    }
    if files.first().unwrap().is_empty() {
        return Ok(());
    }
    let f1 = std::fs::OpenOptions::new().append(true).open(&files[0])?;
    let mut buf1 = std::io::BufWriter::new(f1);

    for file in files.iter().skip(1) {
        if file.is_empty() {
            continue;
        }

        // Skip this file
        if std::fs::metadata(&file).is_err() {
            continue;
        }

        let f2 = std::fs::File::open(&file)?;
        let mut buf2 = std::io::BufReader::new(f2);

        let mut b: Vec<u8> = Vec::new();
        buf2.read_to_end(&mut b)?;
        buf1.write_all(&b)?;
        std::fs::remove_file(&file)?;
    }

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
    async move {
        match fragment_filenames.len() {
            0 => unreachable!(),
            1 => return fragment_filenames[0].clone(),
            2 => {
                // cat!
                cat(&fragment_filenames).unwrap();
                return fragment_filenames[0].clone();
            }
            n => {
                let half = n / 2;
                let left = fragment_filenames[..half].to_vec();
                let right = fragment_filenames[half..].to_vec();
                let handle1 = tokio::spawn(async move { reconstruct_async(left).await });
                let handle2 = tokio::spawn(async move { reconstruct_async(right).await });

                // handle1.await.unwrap();
                let file1 = match handle1.await {
                    Ok(filename) => filename,
                    Err(e) => {
                        eprintln!("Error {e:?}");
                        panic!()
                    }
                };
                let file2 = match handle2.await {
                    Ok(filename) => filename,
                    Err(e) => {
                        eprintln!("Error {e:?}");
                        panic!()
                    }
                };

                let handle3 =
                    tokio::spawn(async move { reconstruct_async(vec![file1, file2]).await });

                match handle3.await {
                    Ok(filename) => filename,
                    Err(e) => {
                        eprintln!("Error {e:?}");
                        panic!();
                    }
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

    log::debug!("NUM_CAT_ONCE = {}", NUM_CAT_ONCE.lock()?);
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
                    panic!("File {filename} does not contain file number.");
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
    // new method
    for (key, val) in map.iter() {
        let fragment_filenames = val.clone();
        let handle = tokio::spawn(async move { reconstruct_async(fragment_filenames).await });
        log::info!("Spawned thread for reconstruct {key}");
        joinhandles.push(handle);
    }

    for handle in joinhandles {
        match handle.await {
            Ok(filename) => {
                log::info!("Filename {filename} reconstruction done.");
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
