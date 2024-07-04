use std::{fmt::Debug, sync::{mpsc::{self, Receiver, Sender}, Arc, Mutex}, thread};

use common::{rdd::{ResultRDD, RDD}, task::Task};

//pub type PoolTask = Box<dyn FnOnce() -> ResultRDD<T,U> + Send + 'static>;

pub struct PoolTaskWrapper<T,U>
where T : Send + Clone + Sync  + serde::ser::Serialize + 'static,
U: Send + 'static 
{
    PoolTask : Box<dyn FnOnce(Task<T>) -> ResultRDD<T, U> + Send + 'static>
}

impl<T,U> PoolTaskWrapper<T,U> 
where T : Send + Clone + Sync + serde::ser::Serialize + 'static,
U: Send + 'static  { 
    pub fn exec_task(self, task : Task<T>) -> ResultRDD<T, U>{ 
        let t = self.PoolTask;
        t(task)
    }
}

pub struct Worker { 
    id: usize,
    pub thread: Option<thread::JoinHandle<()>>
}

impl Worker { 
    pub fn new<T,U> (id: usize, receiver: Arc<Mutex<Receiver<(Task<T>,  PoolTaskWrapper<T,U>)>>>, sender: Arc<Mutex<Sender<ResultRDD<T,U>>>>) -> Self 
    where T : Send + Clone + serde::ser::Serialize + Sync  + 'static,
    U: Send + 'static 
    { 
        let handle = thread::spawn(move || loop { 
            let task = receiver.lock().unwrap().recv();
            match task  {
                Ok((task, t)) => { 
                    let res = t.exec_task(task);
                    sender.lock().unwrap().send(res).unwrap();
                },
                Err(_) => break,
            }
        });
        Self { id, thread: Some(handle) }
    }
}

pub struct ThreadPool<T,U>
where T : Send + Clone + serde::ser::Serialize + Sync + 'static,
U: Send + 'static 
{ 
    pub workers: Vec<Worker>,
    sender: mpsc::Sender<(Task<T>, PoolTaskWrapper<T,U>)>,
    result_receiver: Arc<Mutex<Receiver<ResultRDD<T,U>>>>,
}

impl<T,U> ThreadPool<T,U>
where T : Send + Clone + serde::ser::Serialize + Debug + Sync + 'static,
U: Send + Debug + 'static 
{ 
    pub fn new(cap: usize) -> Self { 
        assert!(cap > 0);
        let (task_sender, task_receiver) = mpsc::channel();
        let (result_sender, result_receiver) = mpsc::channel();
        
        let task_receiver = Arc::new(Mutex::new(task_receiver));
        let result_sender = Arc::new(Mutex::new(result_sender));

        let mut workers = Vec::new();
        for i in 0..cap { 
            let worker = Worker::new(i, Arc::clone(&task_receiver), Arc::clone(&result_sender));
            workers.push(worker);
        }

        ThreadPool { 
            workers, 
            sender: task_sender,
            result_receiver: Arc::new(Mutex::new(result_receiver)),
        }
    }   

    pub fn execute<F>(&self, task : Task<T>, func: F) 
    where F: FnOnce(Task<T>) -> ResultRDD<T,U> + Send + 'static,
    { 
        self.sender.send((task, PoolTaskWrapper{PoolTask:  Box::new(func)})).unwrap();
    }

    pub fn get_results(&self) -> Vec<ResultRDD<T,U>> {
        println!("getting results");
        let result_receiver = self.result_receiver.lock().unwrap();
        let mut results = Vec::new();
        while let Ok(result) = result_receiver.try_recv() {
            println!("result is {:#?}", result);
            results.push(result);
        }
        results
    }
}



#[cfg(test)]
mod tests {
    use common::rdd::{ResultRDD, RDD};
    use common::serializer::Func;
    use common::task::{FuncType, Task};
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    use reqwest::blocking;
    use tokio::runtime::Runtime;

    use super::ThreadPool;
    use std::str::FromStr;
    use std::time::Duration;
    use std::thread;

    #[test] 
    pub fn testThreadPoolfinalmplForTask() { 
        let pool = ThreadPool::new(4);
        let rdd = common::rdd::RDD { data: vec![2,3,4] };
        let func = Func{op : "+".to_string(), value: 4};
        let func_str = serde_json::to_string(&func).unwrap();
        let task = Task::new(rdd.clone(), func_str, FuncType::Map);
        pool.execute(task, move |task| -> ResultRDD<i32, i32> { 
            //let body = serde_json::to_string(&task).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let client = reqwest::Client::new();
                let body = serde_json::to_string(&task).unwrap();
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(HeaderName::from_str("Content-Type").unwrap(), HeaderValue::from_str("application/json").unwrap());
                let res = client.post("http://localhost:8080/execute").headers(headers.clone()).body(body).send().await.unwrap();
                let body_text = res.text().await.unwrap();
                let data: ResultRDD<i32, i32> = serde_json::from_str(&body_text).unwrap();
                data
            })
            
        } );
        let results = pool.get_results();
        for result in results { 
            println!("result {:#?}", result);
        }
    }
}
