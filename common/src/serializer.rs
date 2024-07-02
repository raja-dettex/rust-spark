use serde::{de, Deserialize, Serialize};
use serde::ser::SerializeStruct;


#[derive(PartialEq, Eq, Debug)]
pub struct MapFunc { 
    pub op : String,
    pub value : i32
}


impl Serialize for MapFunc {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        let mut state = serializer.serialize_struct("MapFunc", 2)?;
        state.serialize_field("op", &self.op)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}



impl<'de> Deserialize<'de> for MapFunc {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct MapFuncVisitor;

        impl<'de> de::Visitor<'de> for MapFuncVisitor {
            type Value = MapFunc;

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

                Ok(MapFunc { op, value })
            }
        }

        const FIELDS: &'static [&'static str] = &["op", "value"];
        deserializer.deserialize_struct("MapFunc", FIELDS, MapFuncVisitor)
    }
}



#[cfg(test)]
mod tests {
    use super::MapFunc;
 

    #[test]
    fn test_serde() { 
        let map_func = MapFunc{op: "+".to_string() ,value : 2 };
        let str = serde_json::to_string(&map_func).unwrap();
        println!("{}", str);
        let retrived: MapFunc = serde_json::from_str(&str).unwrap();
        println!("{:?}", retrived);
        assert_eq!(map_func, retrived)
    }
}