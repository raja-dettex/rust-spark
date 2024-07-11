use axum::routing::post;
use axum::Router;
use common::rdd::{ResultRDD, RDD};
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Serialize};
use warp::Filter;

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use axum::extract::Json;
use axum::response::{IntoResponse, Json as JsonResponse};

use common::task::{Execute_I32, Execute_String, FuncType, Task, ToI32, ToString};




async fn handle_task<T, U>(Json(task): Json<Task<T>>) -> impl IntoResponse
where T: Send + Sync + Clone +  'static  + erased_serde::__private::serde::ser::Serialize + Debug + ToI32,
U: Send + Sync + 'static + erased_serde::__private::serde::ser::Serialize+ Debug,
{ 
    println!("here");
    
    let result: ResultRDD<i32, i32> = task.execute_task_i32();
    println!("result {:#?}", result);
    JsonResponse(result)
}


async fn handle_task_string<T, U>(Json(task): Json<Task<T>>) -> impl IntoResponse
where T: Send + Sync + Clone +  'static  + erased_serde::__private::serde::ser::Serialize + Debug + ToString,
U: Send + Sync + 'static + erased_serde::__private::serde::ser::Serialize+ Debug,
{ 
    println!("here");
    
    let result = task.execute_task_string();
    println!("result {:#?}", result);
    JsonResponse(result)
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/execute", post(handle_task::<i32, i32>)).route("/execute/string", post(handle_task_string::<String, String>));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
