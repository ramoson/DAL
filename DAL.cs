using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;
using System.Collections;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Data.OleDb;
namespace DAL
{
    public enum DBEngineType
    {
        MsSql = 1,
        MySql = 2,
        DB2 = 3,
        Oledb = 4
    }

    public class Connection
    {

        private SqlConnection _con;
        private MySqlConnection __con;
        private OleDbConnection ____con;
        private string _con_name;
        private int _con_index;
        private string _con_string;

        public int con_index
        {
            get
            {
                return _con_index;
            }
            set
            {
                try
                {
                    _con_index = value;
                    con_string = ConfigurationManager.ConnectionStrings[con_index].ConnectionString;
                    this.con = new SqlConnection(ConfigurationManager.ConnectionStrings[con_index].ConnectionString);
                }
                catch
                {
                    throw new Exception("Bağlantı kurulamıyor");
                }
            }
        }
        public string con_name
        {
            get
            {
                return _con_name;
            }
            set
            {
                try
                {
                    _con_name = value;
                    con_string = ConfigurationManager.ConnectionStrings[_con_name].ConnectionString;
                    con = new SqlConnection(ConfigurationManager.ConnectionStrings[con_name].ConnectionString);
                }
                catch
                {
                    throw new Exception("Bağlantı kurulamıyor");
                }
            }
        }
        public string con_string
        {
            get
            {
                return _con_string;
            }
            set
            {
                _con_string = value;
                try { _con = new SqlConnection(_con_string); }
                catch { }
                try { __con = new MySqlConnection(_con_string); }
                catch { }
                try { if(_con_string.IndexOf("OLEDB")!=-1) ____con = new OleDbConnection(_con_string); }
                catch { }

            }
        }
        public SqlConnection con
        {
            get
            {
                //if (_con == null && con_name!=null)
                //        _con = new SqlConnection(ConfigurationManager.ConnectionStrings[con_name].ConnectionString);                

                return _con;
            }
            set
            {
                try { _con = value; }
                catch
                {
                    throw new Exception("Bağlantı kurulamıyor");
                }

            }

        }

        public MySqlConnection mycon
        {
            get
            {
                //if (_con == null && con_name!=null)
                //        _con = new SqlConnection(ConfigurationManager.ConnectionStrings[con_name].ConnectionString);                

                return __con;
            }
            set
            {
                try { __con = value; }
                catch
                {
                    throw new Exception("Bağlantı kurulamıyor");
                }

            }

        }
        public OleDbConnection OleDbcon
        {
            get
            {
                //if (_con == null && con_name!=null)
                //        _con = new SqlConnection(ConfigurationManager.ConnectionStrings[con_name].ConnectionString);                

                return ____con;
            }
            set
            {
                try { ____con = value; }
                catch
                {
                    throw new Exception("Bağlantı kurulamıyor");
                }

            }

        }
        public void con_open(DBEngineType tip)
        {
            try
            {
                if (tip == DBEngineType.MsSql)
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                }
                else
                    if (tip == DBEngineType.MySql)
                    {
                        if (mycon.State != ConnectionState.Open)
                            mycon.Open();
                    }
                    else
                        if (tip == DBEngineType.Oledb)
                        {
                            if (OleDbcon.State != ConnectionState.Open)
                                OleDbcon.Open();
                        }
            }
            catch
            {
                throw new Exception("Bağlantı açılamıyor");
            }
        }
        public void con_close(DBEngineType tip)
        {
            try
            {
                if (tip == DBEngineType.MsSql)
                {
                    if (con.State != ConnectionState.Closed)
                        con.Close();
                }
                else
                    if (tip == DBEngineType.MySql)
                    {
                        if (mycon.State != ConnectionState.Closed)
                            mycon.Close();
                    }
                    else
                        if (tip == DBEngineType.Oledb)
                        {
                            if (OleDbcon.State != ConnectionState.Open)
                                OleDbcon.Open();
                        }
            }
            catch
            {
                throw new Exception("Bağlantı açılamıyor");
            }
        }

        public void con_open(object __con, DBEngineType tip)
        {
            try
            {
                if (tip == DBEngineType.MsSql)
                {
                    if ((__con as SqlConnection).State != ConnectionState.Open)
                        (__con as SqlConnection).Open();
                }
                else if (tip == DBEngineType.MySql)
                {
                    if ((__con as MySqlConnection).State != ConnectionState.Open)
                        (__con as MySqlConnection).Open();
                }
                else if (tip == DBEngineType.Oledb)
                {
                    if ((__con as OleDbConnection).State != ConnectionState.Open)
                        (__con as OleDbConnection).Open();
                }
            }
            catch (Exception x)
            {
                throw new Exception("Bağlantı açılamıyor\r\n " + x.ToString());
            }
        }
        public void con_close(object __con, DBEngineType tip)
        {
            try
            {
                if (tip == DBEngineType.MsSql)
                {
                    if ((__con as SqlConnection).State != ConnectionState.Closed)
                        (__con as SqlConnection).Close();
                }
                else if (tip == DBEngineType.MySql)
                {
                    if ((__con as MySqlConnection).State != ConnectionState.Closed)
                        (__con as MySqlConnection).Close();
                }
                else if (tip == DBEngineType.Oledb)
                {
                    if ((__con as OleDbConnection).State != ConnectionState.Closed)
                        (__con as OleDbConnection).Close();
                }
            }
            catch
            {
                throw new Exception("Bağlantı kapatılamıyor");
            }
        }


    }

    public class Database : Connection
    {
        //mssql için tanımlama
        private SqlCommand _cmd;
        private SqlDataAdapter _da;
        private SqlDataReader _dr;
        //mysql için tanımlama
        private MySqlCommand __cmd;
        private MySqlDataAdapter __da;
        private MySqlDataReader __dr;

        private OleDbCommand ____cmd;
        private OleDbDataAdapter ____da;
        private OleDbDataReader ____dr;

        private DBEngineType _dbtip;
        private DataSet _dset;
        private string _text;

        public DBEngineType dbtype
        {
            get
            {
                if (_dbtip == 0)
                    _dbtip = DBEngineType.MsSql;
                return _dbtip;
            }
            set
            {
                _dbtip = value;
            }
        }
        public Database()
        {
//            _dbtip = DBEngineType.MsSql;

            _cmd = new SqlCommand();
            _cmd.Connection = con as SqlConnection;
            _cmd.CommandText = text;
            _cmd.CommandType = cmd_type;

            _da = new SqlDataAdapter();

            __cmd = new MySqlCommand();
            __cmd.Connection = mycon as MySqlConnection;
            __cmd.CommandText = text;
            __cmd.CommandType = cmd_type;

            __da = new MySqlDataAdapter();

            ____cmd = new OleDbCommand();
            ____cmd.Connection = OleDbcon as OleDbConnection;
            ____cmd.CommandText = text;
            ____cmd.CommandType = cmd_type;

            ____da = new OleDbDataAdapter();
        }
        public Database(DBEngineType tip)
        {
            _dbtip = tip;

            if (tip == DBEngineType.MsSql)
            {
                _cmd = new SqlCommand();
                _cmd.Connection = con as SqlConnection;
                _cmd.CommandText = text;
                _cmd.CommandType = cmd_type;
                _da = new SqlDataAdapter();
            }

            else if (tip == DBEngineType.MySql)
            {
                __cmd = new MySqlCommand();
                __cmd.Connection = mycon as MySqlConnection;
                __cmd.CommandText = text;
                __cmd.CommandType = cmd_type;
                __da = new MySqlDataAdapter();
            }
            else if (tip == DBEngineType.Oledb)
            {
                ____cmd = new OleDbCommand();
                ____cmd.Connection = OleDbcon as OleDbConnection;
                ____cmd.CommandText = text;
                ____cmd.CommandType = cmd_type;
                ____da = new OleDbDataAdapter();
            }
            _dset = new DataSet();

            _text = "";

        }
        public SqlCommand cmd
        {
            get
            {

                _cmd.Connection = con;
                _cmd.CommandType = cmd_type;
                _cmd.CommandText = text;
                return _cmd;
            }
            set
            {
                _cmd = value;
            }
        }
        public MySqlCommand mycmd
        {
            get
            {
                __cmd.Connection = mycon;
                __cmd.CommandType = cmd_type;
                __cmd.CommandText = text;
                return __cmd;
            }
            set
            {
                __cmd = value;
            }
        }
        public OleDbCommand OleDbcmd
        {
            get
            {

                ____cmd.Connection = OleDbcon;
                ____cmd.CommandType = cmd_type;
                ____cmd.CommandText = text;
                return ____cmd;
            }
            set
            {
                ____cmd = value;
            }
        }
        public DataColumn[] identity()
        {

            return dset.Tables[0].PrimaryKey;


        }
        public string text
        {
            get
            {
                return _text;
            }
            set
            {

                _text = value;


            }
        }
        public SqlDataReader dreader
        {
            get
            {
                try
                {
                    if (_dr == null)
                    {
                        con_open(cmd.Connection, dbtype);
                        _dr = cmd.ExecuteReader();
                        con_close(cmd.Connection, dbtype);
                    }
                    return _dr;
                }
                catch
                {
                    throw new Exception("Komut yürütülemiyor, bağlantı cümlesini ve sql komutunu kontrol ediniz");
                }

            }
            set
            {
                try
                {
                    con_open(cmd.Connection, dbtype);
                    _dr = cmd.ExecuteReader();
                    con_close(cmd.Connection, dbtype);
                }
                catch
                {
                    throw new Exception("Komut yürütülemiyor, bağlantı cümlesini ve sql komutunu kontrol ediniz");
                }

            }
        }
        public MySqlDataReader mydreader
        {
            get
            {
                try
                {
                    if (__dr == null)
                    {
                        con_open(mycmd.Connection, dbtype);
                        __dr = mycmd.ExecuteReader();
                        con_close(mycmd.Connection, dbtype);
                    }
                    return __dr;
                }
                catch
                {
                    throw new Exception("Komut yürütülemiyor, bağlantı cümlesini ve sql komutunu kontrol ediniz");
                }

            }
            set
            {
                try
                {
                    con_open(mycmd.Connection, dbtype);
                    __dr = mycmd.ExecuteReader();
                    con_close(mycmd.Connection, dbtype);
                }
                catch
                {
                    throw new Exception("Komut yürütülemiyor, bağlantı cümlesini ve sql komutunu kontrol ediniz");
                }

            }
        }
        public OleDbDataReader OleDbdreader
        {
            get
            {
                try
                {
                    if (____dr == null)
                    {
                        con_open(OleDbcmd.Connection, dbtype);
                        ____dr = OleDbcmd.ExecuteReader();
                        con_close(OleDbcmd.Connection, dbtype);
                    }
                    return ____dr;
                }
                catch
                {
                    throw new Exception("Komut yürütülemiyor, bağlantı cümlesini ve sql komutunu kontrol ediniz");
                }

            }
            set
            {
                try
                {
                    con_open(OleDbcmd.Connection, dbtype);
                    ____dr = OleDbcmd.ExecuteReader();
                    con_close(OleDbcmd.Connection, dbtype);
                }
                catch
                {
                    throw new Exception("Komut yürütülemiyor, bağlantı cümlesini ve sql komutunu kontrol ediniz");
                }

            }
        }

        public void add_parameter(String param, Object val)
        {
            if (dbtype == DBEngineType.MsSql)
                cmd.Parameters.AddWithValue(param, val);
            else if (dbtype == DBEngineType.MySql)
                mycmd.Parameters.AddWithValue(param, val);
            else if (dbtype == DBEngineType.Oledb)
                OleDbcmd.Parameters.Add(param, val);

        }
        public SqlDataReader exec()
        {

            try
            {
                con_open(cmd.Connection, dbtype);
                _dr = cmd.ExecuteReader();
                con_close(cmd.Connection, dbtype);
                return _dr;

            }
            catch (Exception x)
            {
                throw new Exception("Bağlantı cümlesini, bağlantı durumunu ve sql cümlesini kontrol ediniz \r\n" + x.ToString());
            }
        }
        public MySqlDataReader myexec()
        {

            try
            {
                con_open(cmd.Connection, dbtype);
                __dr = mycmd.ExecuteReader();
                con_close(mycmd.Connection, dbtype);
                return __dr;

            }
            catch (Exception x)
            {
                throw new Exception("Bağlantı cümlesini, bağlantı durumunu ve sql cümlesini kontrol ediniz \r\n" + x.ToString());
            }
        }
        public OleDbDataReader OleDbexec()
        {

            try
            {
                con_open(OleDbcmd.Connection, dbtype);
                ____dr = OleDbcmd.ExecuteReader();
                con_close(OleDbcmd.Connection, dbtype);
                return ____dr;

            }
            catch (Exception x)
            {
                throw new Exception("Bağlantı cümlesini, bağlantı durumunu ve sql cümlesini kontrol ediniz \r\n" + x.ToString());
            }
        }
        public int exec_non()
        {
            int o = 0;
            try
            {
                if (dbtype == DBEngineType.MsSql)
                {
                    con_open(cmd.Connection, dbtype);
                    o = cmd.ExecuteNonQuery();
                    con_close(cmd.Connection, dbtype);

                }
                else if (dbtype == DBEngineType.MySql)
                {
                    con_open(mycmd.Connection, dbtype);
                    o = mycmd.ExecuteNonQuery();
                    con_close(mycmd.Connection, dbtype);

                }
                else if (dbtype == DBEngineType.Oledb)
                {
                    con_open(OleDbcmd.Connection, dbtype);
                    o = OleDbcmd.ExecuteNonQuery();
                    con_close(OleDbcmd.Connection, dbtype);

                }
            }
            catch (Exception x)
            {
                throw new Exception("Bağlantı cümlesini, bağlantı durumunu ve sql cümlesini kontrol ediniz \r\n" + x.ToString());
            }
            return o;
        }
        public object exec_salar()
        {
            object o = null;
            try
            {
                if (dbtype == DBEngineType.MsSql)
                {
                    con_open(cmd.Connection, dbtype);
                    o = cmd.ExecuteScalar();
                    con_close(cmd.Connection, dbtype);
                }
                else if (dbtype == DBEngineType.MySql)
                {
                    con_open(mycmd.Connection, dbtype);
                    o = mycmd.ExecuteScalar();
                    con_close(mycmd.Connection, dbtype);
                }
                else if (dbtype == DBEngineType.Oledb)
                {
                    con_open(OleDbcmd.Connection, dbtype);
                    o = OleDbcmd.ExecuteScalar();
                    con_close(OleDbcmd.Connection, dbtype);
                }
                return o;
            }
            catch (Exception x)
            {
                throw new Exception("Bağlantı cümlesini, bağlantı durumunu ve sql cümlesini kontrol ediniz \r\n" + x.ToString());
            }
        }
        public object[] exec_salar(char split_char)
        {

            object[] o = new object[10];
            try
            {
                if (dbtype == DBEngineType.MsSql)
                {
                    con_open(cmd.Connection, dbtype);
                    o = cmd.ExecuteScalar().ToString().Split(split_char);
                    con_close(cmd.Connection, dbtype);
                }
                else if (dbtype == DBEngineType.MySql)
                {
                    con_open(mycmd.Connection, dbtype);
                    o = mycmd.ExecuteScalar().ToString().Split(split_char);
                    con_close(mycmd.Connection, dbtype);
                }
                else if (dbtype == DBEngineType.Oledb)
                {
                    con_open(OleDbcmd.Connection, dbtype);
                    o = OleDbcmd.ExecuteScalar().ToString().Split(split_char);
                    con_close(OleDbcmd.Connection, dbtype);
                }
                return o;
            }
            catch (Exception x)
            {
                throw new Exception("Bağlantı cümlesini, bağlantı durumunu ve sql cümlesini kontrol ediniz \r\n" + x.ToString());
            }
        }
        public DataSet dset
        {
            get
            {
                _dset = new DataSet();


                try
                {
                    if (dbtype == DBEngineType.MsSql)
                    {
                        _da = new SqlDataAdapter();
                        _da.SelectCommand = cmd;
                        con_open(cmd.Connection, dbtype);
                        _da.Fill(_dset);
                        con_close(cmd.Connection, dbtype);
                    }
                    else if (dbtype == DBEngineType.MySql)
                    {
                        __da = new MySqlDataAdapter();
                        __da.SelectCommand = mycmd;
                        con_open(mycmd.Connection, dbtype);
                        __da.Fill(_dset);
                        con_close(mycmd.Connection, dbtype);
                    }
                    else if (dbtype == DBEngineType.Oledb)
                    {
                        ____da = new OleDbDataAdapter();
                        ____da.SelectCommand = OleDbcmd;
                        con_open(OleDbcmd.Connection, dbtype);
                        ____da.Fill(_dset);
                        con_close(OleDbcmd.Connection, dbtype);
                    }
                }
                catch (Exception x)
                {
                    throw new Exception("Dataset veri alamıyor, bağlantı cümlesini ve sql komutunu kontrol ediniz \r\n" + x.ToString());
                }
                return _dset;
            }
            set { }
        }
        public DataTable table
        {
            get
            {
                return dset.Tables[0];
            }
        }
        public CommandType cmd_type
        {
            get
            {
                if (dbtype == DBEngineType.MsSql)
                {
                    return _cmd.CommandType;
                }
                else if (dbtype == DBEngineType.MySql)
                {
                    return __cmd.CommandType;
                }
                else if (dbtype == DBEngineType.Oledb)
                {
                    return ____cmd.CommandType;
                }
                else
                    return ____cmd.CommandType;

            }
            set
            {
                if (dbtype == DBEngineType.MsSql)
                {
                    _cmd.CommandType = value;
                }
                else if (dbtype == DBEngineType.MySql)
                {
                    __cmd.CommandType = value;
                }
                else if (dbtype == DBEngineType.Oledb)
                {
                    ____cmd.CommandType = value;
                }

            }
        }
        public string to_html()
        {
            try
            {
                string tab = "";
                StringBuilder st = new StringBuilder();
                st.Append("<table  border='1' cellspacing='0' cellpadding='0'  style='padding:3px; padding:3px; padding:2px; border:1px solid #ff8800; font-family:Arial; font-size:10pt; width:100%;'><tr>");
                foreach (DataColumn dc in table.Columns)
                {
                    st.Append("<th  style='padding:3px; border:1px dotted #cccccc; padding:3px; padding:3px; background-color:#ff8800; color:White; font-family:Arial-Narrow, Arial; font-size:10pt;' align='left'>" + tab + dc.ColumnName + "</th>");
                    tab = "\t";
                }
                st.Append("</tr>\n");
                int i;
                foreach (DataRow dr in table.Rows)
                {
                    tab = "";
                    st.Append("<tr>");
                    for (i = 0; i < table.Columns.Count; i++)
                    {
                        st.Append("<td style='padding:3px; border:1px dotted #cccccc' align='left' valign='top'>" + tab + dr[i].ToString() + "</td>");
                        tab = "\t";
                    }
                    st.Append("</tr>\n");
                }

                return st.Append("</table>").ToString();
            }

            catch (Exception x)
            {
                throw new Exception("Bağlantı cümlesini, bağlantı durumunu ve sql cümlesini kontrol ediniz \r\n" + x.ToString());
            }
        }

    }
    /* SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT,COLUMN_KEY
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE table_name = 'issues'
  AND table_schema = 'bitnami_redmine'  MYSQL
     * 
     * select   tabname,  colname,  typename,  length,  scale,
  default,  nulls,  identity,  keyseq from   syscat.columns where tabname='ABSEQB' DB2
     * */
    public class DataSource : Database
    {
        public string insert, select, update, delete, columns;
        public StringBuilder identity;
        public ArrayList identities;

        public DataSource(string table_name, string cstr_name, DBEngineType tip, bool s_auto, bool u_auto, bool d_auto, bool i_auto)
        {

            select = " "; insert = " "; update = " "; delete = " ";
            StringBuilder str = new StringBuilder();
            StringBuilder str_param = new StringBuilder();
            identity = new StringBuilder();
            identities = new ArrayList();
            var db = new Database(DBEngineType.MsSql);
            identities.Clear();
            db.con_name = cstr_name;
            db.text = "select  syscolumns.name as [Column]  from    sysobjects, syscolumns where sysobjects.id = syscolumns.id and   lower(sysobjects.xtype) = 'u'and   sysobjects.name = @t_name order by syscolumns.name";
            db.add_parameter("@t_name", table_name);
            var db2 = new Database(DBEngineType.MsSql);
            db2.con_name = cstr_name;
            db2.text = "select  syscolumns.name as [Column]  from    sysobjects, syscolumns where sysobjects.id = syscolumns.id and syscolumns.colstat=1 and  lower(sysobjects.xtype) = 'u' and   sysobjects.name = @t_name order by syscolumns.name";
            db2.add_parameter("@t_name", table_name);
            foreach (DataRow dare in db2.table.Rows)
            {
                identities.Add(dare[0].ToString());
                if (identity.Length == 0)
                    identity.Append(String.Format("{0}=@{0}", dare[0].ToString()));
                else
                    identity.Append(String.Format(" and {0}=@{0} ", dare[0].ToString()));
            }
            if (identity.Length > 0)
                delete = String.Format("delete from {0} where {1} ", table_name, identity.ToString());
            else
            {
                foreach (DataRow dare in db.table.Rows)
                {
                    identities.Add(dare[0].ToString());
                    if (identity.Length == 0)
                        identity.Append(String.Format("{0}=@{0}", dare[0].ToString()));
                    else
                        identity.Append(String.Format(" and {0}=@{0} ", dare[0].ToString()));
                }
            }
            delete = String.Format("delete from {0} where {1} ", table_name, identity.ToString());

            foreach (DataRow dr in db.table.Rows)
            {
                if (str.Length == 0)
                {
                    str.Append(dr[0]);
                }
                else
                    str.Append("," + dr[0]);
            }
            select = String.Format("select {0} from {1}", str.ToString(), table_name);
            str_param = new StringBuilder();
            str = new StringBuilder();

            foreach (DataRow dr in db.table.Rows)
            {

                if (!identities.Contains(dr[0].ToString()))
                {
                    if (str.Length == 0)
                    {
                        str.Append(dr[0]);
                        str_param.Append(String.Format("@{0}", dr[0]));
                    }
                    else
                    {
                        str.Append("," + dr[0]);
                        str_param.Append(String.Format(",@{0}", dr[0]));
                    }
                }
            }
            columns = str.ToString();

            insert = (String.Format("insert into {1} ({0}) values({2})", str.ToString(), table_name, str_param.ToString()));
            str_param = new StringBuilder();
            str = new StringBuilder();

            foreach (DataRow dr in db.table.Rows)
            {
                if (!identities.Contains(dr[0].ToString()))
                {
                    if (str_param.Length == 0)
                    {
                        str_param.Append(String.Format("{0}=@{0}", dr[0]));
                    }
                    else
                    {
                        str_param.Append(String.Format(",{0}=@{0}", dr[0]));
                    }
                }
            }

            update = (String.Format("update {1} set {2} where {0}", identity, table_name, str_param.ToString()));
            //else
            //    update = (String.Format("update {1} set {2} ", table_name, str_param.ToString()));


        }
        public DataSource(string table_name, string connection_string, DBEngineType tip, bool s_auto, bool u_auto, bool d_auto, bool i_auto, byte r)
        {
            select = " "; insert = " "; update = " "; delete = " ";
            StringBuilder str = new StringBuilder();
            StringBuilder str_param = new StringBuilder();
            identity = new StringBuilder();
            identities = new ArrayList();

            identities.Clear();
            dbtype = tip;
            con_string = connection_string;
            var db = new Database(tip);
            db.con_string = connection_string;
            if (tip == DBEngineType.MsSql)
                db.text = "select  syscolumns.name as [Column]  from    sysobjects, syscolumns where sysobjects.id = syscolumns.id and   lower(sysobjects.xtype) = 'u'and   sysobjects.name = @t_name order by syscolumns.name";
            else if (tip == DBEngineType.MySql)
                db.text = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE table_name  = @t_name";
            else if (tip == DBEngineType.DB2)
                db.text = "select colname from  syscat.columns where tabname= @t_name";

            db.add_parameter("@t_name", table_name);

            var db2 = new Database(tip);
            db2.con_string = connection_string;
            if (tip == DBEngineType.MsSql)
                db2.text = "select  syscolumns.name as [Column]  from    sysobjects, syscolumns where sysobjects.id = syscolumns.id and syscolumns.colstat=1 and   lower(sysobjects.xtype) = 'u' and   sysobjects.name = @t_name order by syscolumns.name";
            else if (tip == DBEngineType.MySql)
                db2.text = "SELECT COLUMN_NAME  FROM INFORMATION_SCHEMA.COLUMNS  WHERE table_name  = @t_name and COlUMN_KEY='PRI'";
            else if (tip == DBEngineType.DB2)
                db2.text = "select colname from  syscat.columns where tabname= @t_name and (KEYSEQ is not null)";

            db2.add_parameter("@t_name", table_name);
            foreach (DataRow dare in db2.table.Rows)
            {
                identities.Add(dare[0].ToString());
                if (identity.Length == 0)
                    identity.Append(String.Format("{0}=@{0}", dare[0].ToString()));
                else
                    identity.Append(String.Format(" and {0}=@{0} ", dare[0].ToString()));
            }
            if (identity.Length > 0)
                delete = String.Format("delete from {0} where {1} ", table_name, identity.ToString());
            else
            {

                foreach (DataRow dare in db.table.Rows)
                {
                    identities.Add(dare[0].ToString());
                    if (identity.Length == 0)
                        identity.Append(String.Format("{0}=@{0}", dare[0].ToString()));
                    else
                        identity.Append(String.Format(" and {0}=@{0} ", dare[0].ToString()));
                }
            }
            delete = String.Format("delete from {0} where {1} ", table_name, identity.ToString());

            foreach (DataRow dr in db.table.Rows)
            {
                if (str.Length == 0)
                {
                    str.Append(dr[0]);
                }
                else
                    str.Append("," + dr[0]);
            }
            select = String.Format("select {0} from {1}", str.ToString(), table_name);
            str_param = new StringBuilder();
            str = new StringBuilder();

            foreach (DataRow dr in db.table.Rows)
            {

                if (!identities.Contains(dr[0].ToString()))
                {
                    if (str.Length == 0)
                    {
                        str.Append(dr[0]);
                        str_param.Append(String.Format("@{0}", dr[0]));
                    }
                    else
                    {
                        str.Append("," + dr[0]);
                        str_param.Append(String.Format(",@{0}", dr[0]));
                    }
                }
            }
            columns = str.ToString();

            insert = (String.Format("insert into {1} ({0}) values({2})", str.ToString(), table_name, str_param.ToString()));

            str_param = new StringBuilder();
            str = new StringBuilder();

            foreach (DataRow dr in db.table.Rows)
            {
                if (!identities.Contains(dr[0].ToString()))
                {
                    if (str_param.Length == 0)
                    {
                        str_param.Append(String.Format("{0}=@{0}", dr[0]));
                    }
                    else
                    {
                        str_param.Append(String.Format(",{0}=@{0}", dr[0]));
                    }
                }
            }

            update = (String.Format("update {1} set {2} where {0}", identity, table_name, str_param.ToString()));
            //else
            //    update = (String.Format("update {1} set {2} ", table_name, str_param.ToString()));


        }

    }

}

    
   





