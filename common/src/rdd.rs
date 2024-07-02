use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RDD<T> { 
    pub data : Vec<T>
}

impl<T> RDD<T> { 
    pub fn new(data : Vec<T>) -> Self { 
        RDD{data:  data}
    }

    pub fn collect(&self) -> Vec<T> 
    where T : Clone
    { 
        self.data.clone()
    }

    pub fn map<F,U>(&self, func : F) -> RDD<U>
    where F : Fn(&T) -> U   { 
        let data: Vec<U> = self.data.iter().map(func).collect();
        RDD { data  }
    }

    pub fn filter<F>(&self, func : F ) -> Self 
    where F : Fn(&T) -> bool,
    T : Clone
    { 
        let data = self.data.iter().filter(|&x| func(x)).cloned().collect();
        Self{data}
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub enum ResultRDD<T,U> { 
    RDDT(RDD<T>),
    RDDU(RDD<U>)
}

