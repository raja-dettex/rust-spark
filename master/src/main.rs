use std::{str::FromStr, thread::{self, sleep}, time::Duration};
pub mod threads;

use common::{rdd::{ResultRDD, RDD}, serializer::{FilterFunc, Func, StringMapFunc, StringMapOp}, task::{FuncType, Task}};
use reqwest::{blocking::Client, header::{HeaderName, HeaderValue}};
use tokio::runtime::Runtime;
use threads::pool::{execute_i32, execute_string, ThreadPool};



fn main() {
    let int_pool = ThreadPool::new(4);
    let string_pool = ThreadPool::new(4);

    println!("here");

    let rdd = common::rdd::RDD { data: vec![2, 3, 4] };
    let func = Func { op: "+".to_string(), value: 4 };
    let func_str = serde_json::to_string(&func).unwrap();
    let task = Task::new(rdd.clone(), func_str, FuncType::Map);
    
    execute_i32(task, &int_pool);

    let string_rdd = common::rdd::RDD { data: vec!["apple ".to_string(), "mango ".to_string(), "guava ".to_string()] };
    let string_func = StringMapFunc { op : StringMapOp::TrimEnd, value: None};
    let another_func_str = serde_json::to_string(&string_func).unwrap();
    let another_task = Task::new(string_rdd.clone(), another_func_str, FuncType::Map);
    
    execute_string(another_task, &string_pool);

    thread::sleep(Duration::from_secs(5));

    let int_results = int_pool.get_results();
    for result in int_results {
        println!("result {:#?}", result);
    }

    let string_results = string_pool.get_results();
    for result in string_results {
        println!("result {:#?}", result);
    }

    for mut worker in int_pool.workers {
        if let Some(th) = worker.thread.take() {
            th.join().unwrap();
        }
    }

    for mut worker in string_pool.workers {
        if let Some(th) = worker.thread.take() {
            th.join().unwrap();
        }
    }
}


// #[tokio::main]
// async fn main() {


//     // task 1 
//     let rdd = common::rdd::RDD { data: vec![2,3,4] };
//     let func = Func{op : "+".to_string(), value: 4};
//     let func_str = serde_json::to_string(&func).unwrap();
//     let task = Task::new(rdd.clone(), func_str, FuncType::Map);
//     let client = reqwest::Client::new();
//     let body = serde_json::to_string(&task).unwrap();
//     let mut headers = reqwest::header::HeaderMap::new();
//     headers.insert(HeaderName::from_str("Content-Type").unwrap(), HeaderValue::from_str("application/json").unwrap());
//     let res = client.post("http://localhost:8080/execute").headers(headers.clone()).body(body).send().await.unwrap();
//     let body_text = res.text().await.unwrap();
//     let data : ResultRDD<i32, i32> = serde_json::from_str(&body_text).unwrap();
//     match data {
//     ResultRDD::RDDT(d) => println!("data : {:?}", d),
//     ResultRDD::RDDU(d) => println!("data : {:?}", d),
//     } 
        
//     // task 2
//     let filter_func  = FilterFunc {exp : "% 2==0".to_string()};
//     let another_task = Task::new(rdd.clone(), serde_json::to_string(&filter_func).unwrap(), FuncType::Filter);
//     let a_body = serde_json::to_string(&another_task).unwrap();
//     let res_body = client.post("http://localhost:8080/execute").headers(headers.clone()).body(a_body).send().await.unwrap().text().await.unwrap();
//     let a_data : ResultRDD<i32, i32> = serde_json::from_str(&res_body).unwrap();
//     match a_data {
//     ResultRDD::RDDT(d) => println!("data : {:?}", d),
//     ResultRDD::RDDU(d) => println!("data : {:?}", d),
//     }
// }


