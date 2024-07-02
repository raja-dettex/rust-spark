
use crate::rdd::{ResultRDD, RDD};
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Serialize};


use std::sync::{Arc, Mutex};



#[derive(Serialize, Deserialize)]
pub struct Task<T>  
where T : Send + Sync + Serialize + 'static
{ 
    pub rdd : RDD<T>,
    pub func : String,
    pub func_type : FuncType
}







#[derive(Serialize , Deserialize)]
pub enum FuncType { 
    Map,
    Filter
}


impl<T> Task<T> 
where T : Send + Sync + Clone + Serialize + 'static ,

{ 

    pub fn new(rdd : RDD<T>, func : String, func_type : FuncType) -> Self { 
        Self{rdd, func, func_type}
    }
    pub fn execute_task<U>(task : Task<T>) -> ResultRDD<T,U> 
    where U : Send + Sync + 'static
    { 
        match task.func_type {
            FuncType::Map => ResultRDD::RDDT(task.rdd),
            FuncType::Filter => todo!(),
        }
    }
}
