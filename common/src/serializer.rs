
use std::iter::Filter;

use serde::{de, Deserialize, Serialize};
use serde::ser::SerializeStruct;


#[derive(PartialEq, Eq, Debug)]
pub struct Func { 
    pub op : String,
    pub value : i32
}


#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct FilterFunc { 
    pub exp : String
}
#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub enum StringMapOp { 
    Split,
    TrimBeg,
    TrimEnd
}
#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub enum StirngFilterOp { 
    Contains,
    Len
}


#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub struct StringMapFunc { 
    pub op : StringMapOp, 
    pub value : Option<String>
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub struct StringFilterFunc { 
    pub op : StirngFilterOp,
    pub value : String
}

pub struct Wrapper<T,U> { 
    pub func : Box<dyn Fn(&T) -> U >
}

pub trait Construct<T,U> { 
    fn Get(&self) -> Wrapper<T,U>;
}

impl Construct<String, String> for StringMapFunc {
    fn Get(&self) -> Wrapper<String,String> {
        match self.op {
            StringMapOp::Split => { 
                todo!()
            },
            StringMapOp::TrimBeg => Wrapper { func : Box::new(|x : &String| x.trim_start().to_string())},
            StringMapOp::TrimEnd => Wrapper { func : Box::new(|x: &String| x.trim_end().to_string())},
        }
    }
}


impl Construct<String, bool> for StringFilterFunc {
    fn Get(&self) -> Wrapper<String,bool> {
        match self.op {
            StirngFilterOp::Contains => { 
                let value = self.value.clone();
                Wrapper { func : Box::new(move |x : &String| x.contains(&value))}
            },
            StirngFilterOp::Len => { 
                let value = self.value.clone();
                Wrapper{func: Box::new(move |x: &String| x.len() == value.parse::<usize>().unwrap()) }
                
            },
        }
    }
}

impl Construct<i32, bool> for FilterFunc {
    fn Get(&self) -> Wrapper<i32,bool> {
        let a : Vec<&str> = self.exp.split("==").collect();
                let op_str = a.get(0).unwrap().to_string();
                let op : Vec<&str> = op_str.split(" ").collect();
                let operator = op.get(0).unwrap().to_string();
                let value : i32 = op.get(1).unwrap().parse().unwrap();
                let result : i32 = a.get(1).unwrap().parse().unwrap();
                match operator.as_str() { 
                    "%" => { 
                        let func = Box::new(move |x :&i32| x % value == result);
                        return Wrapper{func}
                    },
                    &_ => todo!()
                }
                
    }
}

impl Construct<i32, i32> for Func 
{
    fn Get(&self) -> Wrapper<i32, i32> {
        let val  = self.value;
        
                match self.op.as_str() { 
                    "+" => { 
                        let func  = Box::new(move |x: &i32| x  + val);
                        return Wrapper{func};
                    },
                    "*" => { 
                        let func  = Box::new(move |x: &i32| x  * val);
                        return Wrapper{func};
                    },
                    "/" => { 
                        let func  = Box::new(move |x: &i32| x  / val);
                        return Wrapper{func};
                    },
                    "%" => { 
                        let func  = Box::new(move |x: &i32| x  % val);
                        return Wrapper{func};
                    },
                    
                    &_ => todo!()
                }
          
                
            
        }
        
        
    }



impl Serialize for Func {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        let mut state = serializer.serialize_struct("MapFunc", 2)?;
        state.serialize_field("op", &self.op)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}



impl<'de> Deserialize<'de> for Func {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct MapFuncVisitor;

        impl<'de> de::Visitor<'de> for MapFuncVisitor {
            type Value = Func;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a struct representing a MapFunc")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut op = None;
                let mut value = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "op" => {
                            if op.is_some() {
                                return Err(de::Error::duplicate_field("op"));
                            }
                            op = Some(map.next_value()?);
                        }
                        "value" => {
                            if value.is_some() {
                                return Err(de::Error::duplicate_field("value"));
                            }
                            value = Some(map.next_value()?);
                        }
                        _ => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let op = op.ok_or_else(|| de::Error::missing_field("op"))?;
                let value = value.ok_or_else(|| de::Error::missing_field("value"))?;

                Ok(Func { op, value })
            }
        }

        const FIELDS: &'static [&'static str] = &["op", "value"];
        deserializer.deserialize_struct("MapFunc", FIELDS, MapFuncVisitor)
    }
}



#[cfg(test)]
mod tests {
    use super::Func;
 

    #[test]
    fn test_serde() { 
        let map_func = Func{op: "+".to_string() ,value : 2 };
        let str = serde_json::to_string(&map_func).unwrap();
        println!("{}", str);
        let retrived: Func = serde_json::from_str(&str).unwrap();
        println!("{:?}", retrived);
        assert_eq!(map_func, retrived)
    }
}