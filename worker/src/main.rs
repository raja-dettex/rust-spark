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

use common::task::{Task, FuncType};




async fn handle_task<T,U>(Json(task): Json<Task<T>>) -> impl IntoResponse
where T: Send + Sync + Clone +  'static  + erased_serde::__private::serde::ser::Serialize + Debug,
U: Send + Sync + 'static + erased_serde::__private::serde::ser::Serialize+ Debug,
{ 
    println!("here");
    
    let result: ResultRDD<T, U> = Task::execute_task(task);
    println!("result {:#?}", result);
    JsonResponse(result)
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/execute", post(handle_task::<i32, i32>));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
