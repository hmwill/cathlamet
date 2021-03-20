// MIT License
//
// Copyright (c) 2018-2021 Hans-Martin Will
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::collections::BTreeMap;

use super::Error;
use super::types;
use super::symbols;

/// the logical database, which is a collection of schemata
#[derive(Serialize, Deserialize, Debug)]
pub struct Database {
    pub schemata: BTreeMap<symbols::Name, Schema>,
}

impl Database {
    pub fn new() -> Database {
        Database {
            schemata: BTreeMap::new(),
        }
    }

    pub fn create_schema(&mut self, schema_name: &symbols::Name) -> Result<(), Error> {
        if self.schemata.contains_key(schema_name) {
            return Err(Error::from(format!(
                "Schema '{}' is already defined",
                schema_name
            )));
        }

        let old_value = self.schemata
            .insert(schema_name.clone(), Schema::new(schema_name.clone()));
        assert!(old_value.is_none());
        Ok(())
    }

    pub fn create_table(
        &mut self,
        schema_name: &symbols::Name,
        table_name: &symbols::Name,
        columns: &[Column],
    ) -> Result<(), Error> {
        unimplemented!()
    }

    pub fn create_view(
        &mut self,
        schema_name: &symbols::Name,
        table_name: &symbols::Name,
        query: &str,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    pub fn describe(
        &self,
        schema_name: &symbols::Name,
        object_name: &symbols::Name,
    ) -> Result<RowSet, Error> {
        // validate that the schema name is valid
        let schema = self.schemata
            .get(schema_name)
            .ok_or(Error::from(format!("Schema '{}' not found", schema_name)))?;

        let object = schema.objects.get(object_name).ok_or(Error::from(format!(
            "Object '{}' not found in schema {}",
            object_name, schema_name
        )))?;

        match object {
            &SchemaObject::Table(ref table) => Ok(table.rows.clone()),
            &SchemaObject::View(ref view) => Ok(view.rows.clone()),
        }
    }
}

/// description of a schema within the database
#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    /// the schema name
    pub name: symbols::Name,

    /// the collection of tables making up the database
    pub objects: BTreeMap<symbols::Name, SchemaObject>,
}

impl Schema {
    fn new(name: symbols::Name) -> Self {
        Schema {
            name: name,
            objects: BTreeMap::new(),
        }
    }
}

/// currently, the only schema object types we support are tables and views
#[derive(Serialize, Deserialize, Debug)]
pub enum SchemaObject {
    /// a table object (in-memory B-Tree)
    Table(Table),

    /// a view object (query short-cut)
    View(View),
}

impl SchemaObject {
    pub fn row_set<'a>(&'a self) -> &'a RowSet {
        match self {
            &SchemaObject::Table(ref table) => &table.rows,
            &SchemaObject::View(ref view) => &view.rows,
        }
    }
}

/// Description of a table within the database
#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    /// the name of the table
    pub name: symbols::Name,

    /// description of the data rows that are stored in this table
    pub rows: RowSet,

    /// the names of the columns that are part of the primary key
    pub primary_key: Vec<symbols::Name>,
}

/// Description of a table within the database
#[derive(Serialize, Deserialize, Debug)]
pub struct View {
    /// the name of the view
    pub name: symbols::Name,

    /// description of the data rows that are stored in this table
    pub rows: RowSet,

    /// description of the query used to generate the view (ultimately, this should be an AST)
    pub query: String,
}

/// Description of a collection of rows of the database
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RowSet {
    /// the ordered list of columns in the database
    pub columns: Vec<Column>,
}

impl RowSet {
    pub fn empty() -> Self {
        RowSet { columns: vec![] }
    }

    pub fn meta_data() -> Self {
        RowSet {
            columns: vec![
                Column {
                    name: symbols::Name::from("name"),
                    not_null: true,
                    primary_key: true,
                    data_type: types::DataType::Varchar,
                },
                Column {
                    name: symbols::Name::from("not_null"),
                    not_null: true,
                    primary_key: false,
                    data_type: types::DataType::Numeric,
                },
                Column {
                    name: symbols::Name::from("primary_key"),
                    not_null: true,
                    primary_key: false,
                    data_type: types::DataType::Numeric,
                },
                Column {
                    name: symbols::Name::from("datatype"),
                    not_null: true,
                    primary_key: false,
                    data_type: types::DataType::Varchar,
                },
            ],
        }
    }
}

/// Description of a column with a data set
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    /// the name of the column
    pub name: symbols::Name,

    /// if true, the value is required
    pub not_null: bool,

    /// if true, the column is part of the primary key
    pub primary_key: bool,

    /// the type of the column
    pub data_type: types::DataType,
}
