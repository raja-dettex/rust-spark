use std::str::FromStr;

use common::{rdd::{ResultRDD, RDD}, serializer::MapFunc, task::{FuncType, Task}};
use reqwest::{header::{HeaderName, HeaderValue}, Response};


#[tokio::main]
async fn main() {
    let rdd = common::rdd::RDD { data: vec![2,3,4] };
    let func = MapFunc{op : "*".to_string(), value: 4};
    let func_str = serde_json::to_string(&func).unwrap();
    let task = Task::new(rdd, func_str, FuncType::Map);
    let client = reqwest::Client::new();
    let body = serde_json::to_string(&task).unwrap();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(HeaderName::from_str("Content-Type").unwrap(), HeaderValue::from_str("application/json").unwrap());
    let res = client.post("http://localhost:8080/execute").headers(headers).body(body).send().await.unwrap();
    let body_text = res.text().await.unwrap();
    let data : ResultRDD<i32, i32> = serde_json::from_str(&body_text).unwrap();
    match data {
    ResultRDD::RDDT(d) => println!("data : {:?}", d),
    ResultRDD::RDDU(d) => println!("data : {:?}", d),
    } 
        
    
    
}
