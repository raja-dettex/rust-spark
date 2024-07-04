
use crate::rdd::{ResultRDD, RDD};
use crate::serializer::{Construct, Func, FilterFunc};
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

pub trait ToI32 {
    fn to_i32(&self) -> i32;
}


impl ToI32 for i32 {
    fn to_i32(&self) -> i32 {
        *self
    }
}

impl ToI32 for String {
    fn to_i32(&self) -> i32 {
        self.parse().unwrap_or(0)
    }
}

pub trait FromI32: Sized {
    fn from_i32(value: i32) -> Self;
}

// Implement FromI32 for i32
impl FromI32 for i32 {
    fn from_i32(value: i32) -> Self {
        value
    }
}


#[derive(Serialize , Deserialize)]
pub enum FuncType { 
    Map,
    Filter
}


impl<T> Task<T> 
where T : ToI32 +  Send + Sync + Clone + Serialize + 'static,

{ 

    pub fn new(rdd : RDD<T>, func : String, func_type : FuncType) -> Self { 
        Self{rdd, func, func_type}
    }
    pub fn execute_task<U>(task : Task<T>) -> ResultRDD<i32,i32> 
    where U : Send + Sync + 'static
    { 
        match task.func_type {
            FuncType::Map => {
                let map_func: Func = serde_json::from_str(&task.func).unwrap();
                let func = map_func.Get().func;
                let result = task.rdd.map(|x| func(&(x.to_i32())));
                ResultRDD::RDDU(result)
            },
            FuncType::Filter => { 
                let filter_func :FilterFunc = serde_json::from_str(&task.func).unwrap();
                let func = filter_func.Get().func;
                let result = task.rdd.filter(|x| func(&(x.to_i32()))).map(|x| (*x).to_i32());
                ResultRDD::RDDT(result)
            },
        }
    }
}
