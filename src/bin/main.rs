/*
 * TODO
 * =========
 * - Skip JSON serialization for database
 *     - This was copied over from the original function + easier to implement
 */


use std::env;
use std::fs;
// use std::io;
use std::io::Write;
use std::io::Read;
// use std::io::Seek;
use std::fs::File;
// use zip_rs::ZipArchive;
// use zip::ZipArchive;
// use flate2::Compression;
use std::collections::{HashSet, HashMap};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
// use std::thread;
use std::time::SystemTime;
use main_error::MainError;
use serde::{Deserialize, Serialize};
use tf_demo_parser::demo::header::Header;
use tf_demo_parser::demo::parser::analyser::MatchState;
use tf_demo_parser::{Demo, DemoParser};
use log::{info, error};
use zmq::{Context, Socket};
use std::thread;


#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonDemo {
    header: HeaderWithFilename,
    #[serde(flatten)]
    state: MatchState,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonDemoNoNesting {
    filename: String,
    header: Header,
    #[serde(flatten)]
    state: MatchState,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeaderWithFilename {
    filename: String,
    #[serde(flatten)]
    header: Header,
}


fn distribute_compression(file_list: HashSet<String>) -> Result<(), MainError> {

    // set up send/receive sockets for distribute jobs
    let push_socket = env::var("PUSH_SOCKET").unwrap_or_else(|_| "6578".to_string()); // this language is retarded
    let pull_socket = env::var("PULL_SOCKET").unwrap_or_else(|_| "6579".to_string());

    let context = Context::new();


    // Fetch worker list from GitHub
    // let worker_list_url = "https://raw.githubusercontent.com/potato-tf/deploy/main/serverlist.json";
    // let response = reqwest::blocking::get(worker_list_url)?.text()?;

    // let server_data: serde_json::Value = serde_json::from_str(&response)
    //     .expect("Failed to parse JSON");

    // let mut worker_hosts = Vec::new();
    // if let Some(servers) = server_data["servers"].as_array() {
    //     for server in servers {
    //         if let Some(server_str) = server.as_str() {
    //             // Parse "port user@host" format and extract just the host
    //             let parts: Vec<&str> = server_str.split_whitespace().collect();
    //             if parts.len() >= 2 {
    //                 let user_host = parts[1];
    //                 if let Some(host) = user_host.split('@').nth(1) {
    //                     worker_hosts.push(host.to_string());
    //                 }
    //             }
    //         }
    //     }
    // }
    let worker_hosts = vec!["127.0.0.1"];
    // println!("Found {} worker hosts: {:?}", worker_hosts.len(), worker_hosts);
    // info!("Found {} worker hosts: {:?}", worker_hosts.len(), worker_hosts);

    let sender = context.socket(zmq::PUSH).unwrap();
    sender.connect(&format!("tcp://*:{}", push_socket));

    let receiver = context.socket(zmq::PULL).unwrap();
    receiver.connect(&format!("tcp://*:{}", pull_socket));

    for file in file_list.iter() {
        println!("Sending compression job for {}", file);
        info!("Sending compression job for {}", file);
        let job = serde_json::json!({
            "file": file,
            "output": format!("{}.xz", file)
        });
        sender.send(&job.to_string(), 0).unwrap();
    }

    let mut completed_jobs = 0;

    println!("Waiting for {} jobs to complete", file_list.len());
    info!("Waiting for {} jobs to complete", file_list.len());

    while completed_jobs < file_list.len() {
        let msg = receiver.recv_string(0).unwrap().unwrap();
        completed_jobs += 1;
        println!("Completed compression for {}", msg);
        info!("Completed compression for {}", msg);
    }

    Ok(())
}
 
fn batchparse(processed_files: &mut HashSet<String>) -> Result<(), MainError> {

    let args: Vec<_> = env::args().collect();
    let all = args.contains(&std::string::String::from("all"));
    let main_dir = fs::read_dir(".")?;
    let mut parsed_files = HashMap::new();
 
    for entry in main_dir {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        let path = entry.path();

        if path.is_dir() {

            let file_path = format!("{}/all_demos_new.json", path.to_str().unwrap());

            info!("Parsing demos in {:?}", path);
            println!("Parsing demos in {:?}", path);
            let mut output_file = File::create(&file_path)?;
            write!(output_file, "[")?;

            let subdir = match fs::read_dir(&path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for sub_entry in subdir {

                let sub_entry = match sub_entry {
                    Ok(entry) => entry,
                    Err(_) => continue,
                };
                let sub_path = sub_entry.path();

                if sub_path.is_file() && sub_path.extension().unwrap_or_default() == "zip" {

                    let file = match fs::File::open(&sub_path) {
                        Ok(file) => file,
                        Err(_) => continue,
                    };
                    let mut archive = match zip::ZipArchive::new(file) {
                        Ok(archive) => archive,
                        Err(_) => continue,
                    };

                    for i in 0..archive.len() {
                        if sub_path.with_extension("json").exists() {
                            continue;
                        }

                        let mut compressed_file = match archive.by_index(i) {
                            Ok(file) => file,
                            Err(_) => continue,
                        };

                        let mut contents = Vec::new();
                        if let Err(_) = compressed_file.read_to_end(&mut contents) {
                            continue;
                        }

                        let dem_path = sub_path.with_extension("dem");
                        let mut dem_file = match fs::File::create(&dem_path) {
                            Ok(file) => file,
                            Err(_) => continue,
                        };
                        if let Err(_) = dem_file.write_all(&contents) {
                            continue;
                        }

                        let demo_file_contents = match fs::read(&dem_path) {
                            Ok(contents) => contents,
                            Err(_) => continue,
                        };
                        let demo = Demo::new(&demo_file_contents);

                        let parser = if all {
                            DemoParser::new_all(demo.get_stream())
                        } else {
                            DemoParser::new(demo.get_stream())
                        };
                        let (header, state) = match parser.parse() {
                            Ok((header, state)) => (header, state),
                            Err(_) => continue,
                        };
                        let file_stem = sub_path.file_stem().and_then(|s| s.to_str()).unwrap_or("Unknown");
                        // let filename = file_stem.to_string();

                        let json_demo = JsonDemo { header: HeaderWithFilename { filename: file_stem.to_string(), header }, state };

                        if !processed_files.contains(&json_demo.header.filename) {
                            if processed_files.is_empty() {
                                write!(output_file, "{}", serde_json::to_string(&json_demo)?)?;
                            } else {
                                write!(output_file, ",{}", serde_json::to_string(&json_demo)?)?;
                            }
                            info!("Writing: {:?}", &json_demo.header.filename);
                            println!("Writing: {:?}", &json_demo.header.filename);
                        }

                        parsed_files.insert(json_demo.header.filename, dem_path.clone().to_str().unwrap().to_string());

                        if let Err(_) = fs::remove_file(&sub_path) {
                            continue;
                        }
                    }
                }
            }
            
            write!(output_file, "]");
            // Generate timestamp
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            // Specify the directory where the zip file will be saved
            let parsed_old_dir = PathBuf::from(format!("{}/parsed_old", path.to_str().unwrap()));
            fs::create_dir_all(&parsed_old_dir)?; // Create the directory if it doesn't exist

            // Copy the current JSON file to the archive location with timestamp
            let timestamped_json = parsed_old_dir.join(format!("all_demos_{}.json", timestamp));
            fs::copy("all_demos.json", &timestamped_json)?;

            // Use xz command to compress the file with maximum compression
            let output = std::process::Command::new("nice")
                .arg("-n")
                .arg("18")
                .arg("xz")
                .arg("-9")
                .arg(&timestamped_json)
                .output()?;

            if !output.status.success() {
                let errmsg = String::from_utf8_lossy(&output.stderr);
                if !errmsg.ends_with("File exists") {
                    error!("Failed to compress file: {}", errmsg);
                }
            }

            // Remove the old JSON file
            if let Err(err_) = fs::remove_file("all_demos.json") {
                error!("{:?}", err_);
            }

            // Overwrite old JSON file with new data
            fs::rename(file_path, "all_demos.json")?;
        }
    }
    Ok(())
 
 
     // Compress parsed demos
     // info!("Compressing parsed demos...");
     // println!("Compressing parsed demos...");
     // for (_key, value) in &parsed_files {
 
     //     let dem_path = PathBuf::from(value);
     //     if !dem_path.exists() {
     //         error!("Demo path does not exist (how?): {}", value);
     //         continue;
     //     }
 
     //     let compression_level = env::var("DEM_COMPRESSION_LEVEL").unwrap_or_else(|_| "-9".to_string());
 
     //     println!("Compressing demo {:?}", dem_path);
     //     info!("Compressing demo {:?}", dem_path);
     //     let output = std::process::Command::new("nice")
     //         .arg("-n")
     //         .arg("18")
     //         .arg("xz")
     //         .arg(compression_level)
     //         .arg(dem_path)
     //         .output()?;
     //     if !output.status.success() {
     //         let errmsg = String::from_utf8_lossy(&output.stderr);
     //         error!("Failed to compress file: {}", errmsg);
     //     }
     // }
     // println!("Done compressing parsed demos");
     // info!("Done compressing parsed demos");
}

fn main() -> Result<(), MainError> {

    env_logger::init();

    let demos_dir = env::var("DEMOS_DIR").unwrap_or_else(|_| "demos".to_string());
    let new_dir = env::current_dir().unwrap().join(demos_dir);
    env::set_current_dir(&new_dir).unwrap();

    let path = Path::new("all_demos.json");
    if !path.exists() {
        OpenOptions::new().write(true).create_new(true).open(path)?;
    }

    let mut processed_files = HashSet::new();
    batchparse(&mut processed_files)?;
    distribute_compression(processed_files)?;
    Ok(())
}

