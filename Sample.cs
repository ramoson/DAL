 Database db = new Database();
                db.con_name = BAS.Connection.ConnectionName;
                db.text = "select sdb.DataSource  from STablolar as st left join SDBBaglantilar as sdb on st.BAGLANTIID=sdb.ID where st.AD=@ad";
                db.add_parameter("@ad", table);
                string connection = db.table.Rows[0][0].ToString();
