# DAL
Data Acces Library for Mssql and Mysql in c#

You can add this library to your .net project then you can get data from MsSql or MySql DB easly.

Like that;

Database db=new Database(DBEngineType.Mssql);
db.con_string="connection string";
db.text="select * from table";
db.table; // The data receiving as DataTable;
db.exec_non(); //ExecuteNonQuery
db.exec();// ExecuteQuery
