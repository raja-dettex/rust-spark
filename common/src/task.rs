
use crate::rdd::{ResultRDD, RDD};
use crate::serializer::{Construct, Func, FilterFunc, StringMapFunc, StringFilterFunc};
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

pub trait ToString { 
    fn to_string(&self) -> String;
}

impl ToString for String {
    fn to_string(&self) -> String {
        self.clone()
    }
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

pub trait Execute_I32<T> 
where T : ToI32  +  Send + Sync + Clone + Serialize + 'static { 
    fn execute_task_i32(&self) -> ResultRDD<i32, i32>;
}
pub trait Execute_String<T> 
where T : ToString  +  Send + Sync + Clone + Serialize + 'static { 
    fn execute_task_string(&self) -> ResultRDD<String, String>;
}




impl<T> Task<T> 
where T :  Send + Sync + Clone + Serialize + 'static,

{ 

    pub fn new(rdd : RDD<T>, func : String, func_type : FuncType) -> Self { 
        Self{rdd, func, func_type}
    }
    // pub fn execute_task<U>(task : Task<T>) -> ResultRDD<i32,i32> 
    // where U : Send + Sync + 'static
    // { 
    //     match task.func_type {
    //         FuncType::Map => {
    //             let map_func: Func = serde_json::from_str(&task.func).unwrap();
    //             let func = map_func.Get().func;
    //             let result = task.rdd.map(|x| func(&(x.to_i32())));
    //             ResultRDD::RDDU(result)
    //         },
    //         FuncType::Filter => { 
    //             let filter_func :FilterFunc = serde_json::from_str(&task.func).unwrap();
    //             let func = filter_func.Get().func;
    //             let result = task.rdd.filter(|x| func(&(x.to_i32()))).map(|x| (*x).to_i32());
    //             ResultRDD::RDDT(result)
    //         },
    //     }
    // }

    // pub fn execute_string_task<U>(task : Task<T>) -> ResultRDD<String, String> { 
    //     match task.func_type {
    //         FuncType::Map => { 
    //             let map_func: StringMapFunc = serde_json::from_str(&task.func).unwrap();
    //             let func = map_func.Get().func;
    //             let result = task.rdd.map(|x| func(&(x.to_string())));
    //             ResultRDD::RDDU(result)
    //         },
    //         FuncType::Filter => { 
    //             let filter_func: StringFilterFunc = serde_json::from_str(&task.func).unwrap();
    //             let func = filter_func.Get().func;
    //             let result = task.rdd.filter(|x| func(&(x.to_string()))).map(|x| x.to_string());
    //             ResultRDD::RDDT(result)
    //         },
    //     }        
    // }
}


impl<T> Execute_I32<T> for Task<T>
where T : ToI32 + Send + Clone + Sync + Serialize +  'static {
    fn execute_task_i32(&self) -> ResultRDD<i32, i32> {
        match self.func_type {
            FuncType::Map => {
                let map_func: Func = serde_json::from_str(&self.func).unwrap();
                let func = map_func.Get().func;
                let result = self.rdd.map(|x| func(&(x.to_i32())));
                ResultRDD::RDDU(result)
            },
            FuncType::Filter => { 
                let filter_func :FilterFunc = serde_json::from_str(&self.func).unwrap();
                let func = filter_func.Get().func;
                let result = self.rdd.filter(|x| func(&(x.to_i32()))).map(|x| (*x).to_i32());
                ResultRDD::RDDT(result)
            },
        }
    }
}
impl<T> Execute_String<T> for Task<T>
where T : ToString + Send + Clone + Sync + Serialize +  'static {
    fn execute_task_string(&self) -> ResultRDD<String, String> {
        match self.func_type {
            FuncType::Map => {
                let map_func: StringMapFunc = serde_json::from_str(&self.func).unwrap();
                let func = map_func.Get().func;
                let result = self.rdd.map(|x| func(&(x.to_string())));
                ResultRDD::RDDU(result)
            },
            FuncType::Filter => { 
                let filter_func :StringFilterFunc = serde_json::from_str(&self.func).unwrap();
                let func = filter_func.Get().func;
                let result = self.rdd.filter(|x| func(&(x.to_string()))).map(|x| (*x).to_string());
                ResultRDD::RDDT(result)
            },
        }
    }
}