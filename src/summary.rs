use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Summary {
    query: String,
    count: i32,
    total: f64,
    ratio_max: f64,
    ratio_min: f64,
    lock_time: f64,
}

pub fn generate_report(config_path: &Path, output_file: &Path) -> Result<(), Box<dyn Error>> {
    match File::open(config_path) {
        Err(why) => Err(format!("couldn't open {}: {}", config_path.display(), why).into()),
        Ok(file) => {
            let mut map: HashMap<String, Summary> = HashMap::new();
            let reader = BufReader::new(file);

            let mut file_write = File::create(output_file).unwrap();

            for line in reader.lines() {
                let line = line?;
                let parsed: Value = serde_json::from_str(&line).expect("JSON parsing failed");

                let digest = format!("{:x}", md5::compute(parsed["replaced_query"].to_string()));

                let ratio =
                    parsed["row_examined"].as_f64().unwrap() / parsed["row_sent"].as_f64().unwrap();

                if let Some(q) = map.get_mut(&digest) {
                    q.count += 1;
                    q.total += parsed["query_time"].as_f64().unwrap();
                    if ratio > q.ratio_max {
                        q.ratio_max = ratio;
                    }else if ratio < q.ratio_min {
                        q.ratio_min = ratio;
                    }
                } else {
                    map.insert(
                        digest,
                        Summary {
                            query: parsed["replaced_query"].to_string(),
                            count: 1,
                            total: parsed["query_time"].as_f64().unwrap(),
                            ratio_max: ratio,
                            ratio_min: ratio,
                            lock_time: parsed["lock_time"].as_f64().unwrap()
                        },
                    );
                }
            }

            for (key, value) in &map {
                let _ = writeln!(
                    &mut file_write,
                    "{}\t{:15.4}\t{:15.4}\t{:<width$}\t{:15.4}\t{:15.4}\t{:15.4}\t{}",
                    key,
                    value.total / value.count as f64,
                    value.lock_time / value.count as f64,
                    value.count,
                    value.total,
                    value.ratio_min,
                    value.ratio_max,
                    value.query,
                    width = 10
                );
            }

            // let _ = File::write(
            //     &mut file_write,
            //     serde_json::to_string(&map).unwrap().as_bytes(),
            // );

            let _ = File::flush(&mut file_write);

            Ok(())
        }
    }
}
