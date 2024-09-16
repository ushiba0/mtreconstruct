extern crate env_logger;
extern crate getopts;
extern crate log;

use once_cell::sync::Lazy;
use regex::Regex;
use std::env;
use std::fs::{self, DirEntry};
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

const NUM_CAT_ONCE_DEFATLT: usize = 32;
static NUM_CAT_ONCE: Lazy<Mutex<usize>> = Lazy::new(|| Mutex::new(NUM_CAT_ONCE_DEFATLT));

struct VisitDir {
    root: Box<dyn Iterator<Item = io::Result<DirEntry>>>,
    children: Box<dyn Iterator<Item = VisitDir>>,
}

impl VisitDir {
    fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let root = Box::new(fs::read_dir(&path)?);
        let children = Box::new(fs::read_dir(&path)?.filter_map(|e| {
            let e = e.ok()?;
            if e.file_type().ok()?.is_dir() {
                return VisitDir::new(e.path()).ok();
            }
            None
        }));
        Ok(VisitDir { root, children })
    }

    fn entries(self) -> Box<dyn Iterator<Item = io::Result<DirEntry>>> {
        Box::new(self.root.chain(self.children.flat_map(|s| s.entries())))
    }
}

impl Iterator for VisitDir {
    type Item = io::Result<DirEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.root.next() {
            return Some(item);
        }
        if let Some(child) = self.children.next() {
            self.root = child.entries();
            return self.next();
        }
        None
    }
}

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

#[derive(Debug)]
struct Task {
    files: Vec<String>,
    handler: std::thread::JoinHandle<()>,
}

impl Task {
    fn new() -> Task {
        let handler = std::thread::spawn(|| {});
        Task {
            files: Vec::new(),
            handler,
        }
    }
}

fn reconstruct(file: &String, fragments: &[String]) {
    log::info!("Start reconstructing {}", file);
    let num_cat_once = *NUM_CAT_ONCE.lock().unwrap();
    let mut fragments = fragments.to_vec();
    fragments.reverse();

    // Do leaf tasks.
    let mut leaf_tasks: Vec<Task> = Vec::new();
    loop {
        let mut task = Task::new();
        for _ in 0..num_cat_once {
            let f = fragments.pop().unwrap_or_default();
            task.files.push(f.clone());
        }
        let files = task.files.to_vec();
        if files.first().unwrap().is_empty() {
            break;
        }
        task.handler = std::thread::spawn(move || {
            loop {
                match cat(&files) {
                    Ok(_) => break,
                    Err(error) => {
                        log::debug!(
                            "Error: {}. Retrying in 5 secs. Leader = {}",
                            error,
                            files[0]
                        );
                        std::thread::sleep(std::time::Duration::from_secs(5));
                    }
                }
            }
            //cat(&files).unwrap();
        });
        leaf_tasks.push(task);
    }

    // Do sectoin tasks.
    loop {
        if leaf_tasks.len() <= 1 {
            break;
        }
        let mut temp_tasks: Vec<Task> = Vec::new();
        leaf_tasks.reverse();

        loop {
            let mut task = Task::new();
            let mut child_tasks: Vec<Task> = Vec::new();

            for _ in 0..num_cat_once {
                let t = leaf_tasks.pop().unwrap_or_else(Task::new);
                task.files
                    .push(t.files.first().unwrap_or(&String::from("")).clone());
                child_tasks.push(t);
            }
            let files = task.files.to_vec();
            task.handler = std::thread::spawn(move || {
                for i in child_tasks {
                    i.handler.join().unwrap();
                }
                loop {
                    match cat(&files) {
                        Ok(_) => break,
                        Err(error) => {
                            log::debug!(
                                "Error: {}. Retrying in 5 secs. Leader = {}",
                                error,
                                files[0]
                            );
                            std::thread::sleep(std::time::Duration::from_secs(6));
                        }
                    }
                }
                //cat(&files).unwrap();
            });
            temp_tasks.push(task);

            if leaf_tasks.is_empty() {
                break;
            }
        }

        assert_eq!(leaf_tasks.len(), 0);
        leaf_tasks.append(&mut temp_tasks);
    }

    let last_task = leaf_tasks.pop().unwrap();
    assert_eq!(leaf_tasks.len(), 0);

    // Make sure last task has been finished.
    last_task.handler.join().unwrap();

    // Rename vsi_traverse_-s--l-0.txt.FRAG-00000
    // e.g. rename vsi_traverse_-s--l-0.txt.FRAG-00000 to vsi_traverse_-s--l-0.txt
    let long_filename = last_task.files.first().unwrap().clone();
    let short_filename = file.clone();
    std::fs::rename(&long_filename, &short_filename).unwrap();

    log::info!("End reconstruction of {}", file);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    parse_args()?;
    env_logger::init();

    log::debug!("NUM_CAT_ONCE = {}", NUM_CAT_ONCE.lock()?);

    let re = Regex::new(r".FRAG-")?;
    let timer = std::time::Instant::now();

    // Find files to reconstruct.
    let paths = VisitDir::new(".")?
        .filter_map(|e| Some(e.ok()?.path().to_string_lossy().into_owned()))
        .filter(|s| re.is_match(s))
        .collect::<Vec<_>>();

    let mut map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    for i in paths.iter() {
        let file: String = i.split(".FRAG-").next().unwrap().to_string();
        map.entry(file)
            .and_modify(|files| files.push(i.to_string()))
            .or_insert_with(|| vec![i.to_string()]);
    }

    let mut join_handler = Vec::new();

    for (key, val) in &mut map {
        val.sort_unstable();
        let key_copy = key.clone();
        let val_copy = val.to_vec();
        let handler = std::thread::spawn(move || {
            reconstruct(&key_copy, &val_copy);
        });
        join_handler.push(handler);
    }

    for i in join_handler {
        i.join().unwrap();
    }

    log::info!(
        "Reconstruction completed. Elapsed {} ms",
        timer.elapsed().as_millis()
    );
    Ok(())
}
