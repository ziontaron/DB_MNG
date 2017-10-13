
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Globalization;
using System.Data.OleDb;
using System.IO;
using System.Data.SqlClient;
using System.Net.Mail;

namespace BD_MNG
{
    public class BD_MNG
    {
        private string Command = "";
        private string DB_file = "";
        private string archivodb = "";
        private OleDbConnection conector;
        private OleDbCommand OleCommand;
        private string exeption = "";
        private bool error = false;

        ///Propiedades/////////////////////////////////////////////////
        #region Propiedades
        public string Command_Query
        {
            get
            {
                return Command;
            }
            set
            {
                if (value != Command) Command = value;
            }
        }
        public string DB_File
        {
            get
            {
                return DB_file;
            }
            set
            {
                if (value != DB_file) DB_file = value;
            }
        }
        public string Connection_String
        {
            get
            {
                return archivodb;
            }
            set
            {
                if (value != archivodb) archivodb = value;
            }
        }
        public string Exeption
        {
            get
            {
                return exeption;
            }
            //set
            //{
            //    if (value != exeption) exeption = value;
            //}
        }
        public bool Error
        {
            get
            {
                return error;
            }
            //set
            //{
            //    throw new System.NotImplementedException();
            //}
        }
        #endregion

        ///Costructor//////////////////////////////////////////////////
        #region Costructores
        public BD_MNG(string Data_Base)
        {
            DB_file = Data_Base;
            conector = new OleDbConnection(Build_ConStr());
            error = false;
        }
        public BD_MNG(string Data_Base,string Query)
        {
            Command = Query;
            DB_file = Data_Base;
            conector = new OleDbConnection(Build_ConStr());
            error = false;
        }
        #endregion

        ///Funciones///////////////////////////////////////////////////
        #region Funciones
        private string Build_ConStr()///construye el Connection String
        {
            string conn07 = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=";
            string conn00 = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=";
            string connection = DB_file;
            if (connection.Contains(".accdb")) connection = conn07 + connection;
            if (connection.Contains(".mdb")) connection = conn00 + connection;
            archivodb = connection;
            //string archivodb = ("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + DB_file);
            return archivodb;
        }
        private void Build_Comm()/// construye OLE COMMAND
        {
            OleCommand = new OleDbCommand(Command, conector);
        }
        ///USER FUNCIONS
        public void Load_Command(string DB_Command)///Carga el comando SQL
        {
            Command = DB_Command;
            Build_Comm();
        }
        public bool Execute_Command()///Ejecuta Commando que no sea Query
        {
            exeption = "";
            error = false;
            try
            {
                conector.Open();
                OleCommand.ExecuteNonQuery();
                conector.Close();
                return true;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return false;
            }
        }
        public bool Execute_Command(string DB_command)///Ejecuta Commando que no sea Query
        {
            exeption = "";
            Command = DB_command;
            Build_Comm(); 
            error = false;
            try
            {
                conector.Open();
                OleCommand.ExecuteNonQuery();
                conector.Close();
                return true;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return false;
            }
        }
        public DataTable Execute_Query()///Ejecuta Query y regresa una tabla llena
        {
            exeption = "";
            DataTable tabla_Query = new DataTable();
            error = false;
            try
            {
                conector.Open();
                OleDbDataAdapter adaptador = new OleDbDataAdapter(Command, conector);
                OleDbCommandBuilder actualizardb = new OleDbCommandBuilder(adaptador);
                adaptador.Fill(tabla_Query);
                conector.Close();
                return tabla_Query;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                error = true;
                conector.Close();
                return tabla_Query;
            }
        }
        public DataTable Execute_Query(string Query)///Ejecuta Query y regresa una tabla llena
        {
            exeption = "";
            Command = Query;
            Build_Comm();
            error = false;
            DataTable tabla_Query = new DataTable();
            try
            {
                conector.Open();
                OleDbDataAdapter adaptador = new OleDbDataAdapter(Command, conector);
                OleDbCommandBuilder actualizardb = new OleDbCommandBuilder(adaptador);
                adaptador.Fill(tabla_Query);
                conector.Close();
                return tabla_Query;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return tabla_Query;
            }
        }
        public DataTable Schematic_of_Tables()
        {
            DataTable Schematictable = new DataTable();
            conector.Open();
            Schematictable = conector.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
            conector.Close();
            return Schematictable;
        }///entrega las tablas que existen en la BD
        public DataTable Schematic_of_Columns(string DB_table)
        {
            DataTable Schematictable = new DataTable();
            conector.Open();
            Schematictable = conector.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, DB_table, null });
            conector.Close();
            return Schematictable;
        }///entrega los campos que existen en la BD
        #endregion
    }
    public class SQL_DTMG
    {
        //sql_con = "Data Source=" + server + ";Initial Catalog=FSDBMR;Persist Security Info=True;User ID=" + user + ";Password=" + pass;
        #region Variables Privadas
        private string _server = "";
        private string _user = "";
        private string _pass = "";
        private string _dbname = "";
        private string _sql_con_str = "";
        private SqlConnection _connection;
        private SqlParameter[] _parameters;
        private SqlCommand _command;
        private SqlDataAdapter _adapter;
        private string _script = "";
        private string _error_mjs = "";
        #endregion

        #region Variales Publicas
        public string Command
        {
            get
            {
                return _script;
            }
            set
            {
                if (value != _script) _script = value;
            }
        }
        public string Server
        {
            get
            {
                return _server;
            }
            set
            {
                if (value != _server) _server = value;
            }
        }
        public string DB_Name
        {
            get
            {
                return _dbname;
            }
            set
            {
                if (value != _dbname) _dbname = value;
            }
        }
        public string User
        {
            get
            {
                return _user;
            }
            set
            {
                if (value != _user) _user = value;
            }
        }
        public string Pass
        {
            get
            {
                return _pass;
            }
            set
            {
                if (value != _pass) _pass = value;
            }
        }
        public string Error_Mjs
        {
            get
            {
                return _error_mjs;
            }
            set
            {
                if (value != _error_mjs) _error_mjs = value;
            }
        }
        #endregion

        #region Constructores
        public SQL_DTMG(string SQL_Connection_String)
        {
            _sql_con_str = SQL_Connection_String;
            _connection = new SqlConnection(_sql_con_str);
        }
        public SQL_DTMG(string Server, string DBname, string User, string Pass)
        {
            _sql_con_str = "Data Source=" + Server + ";Initial Catalog=" + DBname +
                ";Persist Security Info=True;User ID=" + User + ";Password=" + Pass;
            _connection = new SqlConnection(_sql_con_str);
        }
        #endregion

        #region Funciones Privadas
        private void Start_Connection()
        {
            _connection.Open();
        }
        private void Stop_Connection()
        {
            _connection.Close();
        }
        private void Build_Command()
        {
            _command = new SqlCommand(_script, _connection);

            //SqlParameter[] sqlparams2 = new SqlParameter[2];
            //SqlParameter[] sqlparams = { new SqlParameter(), new SqlParameter(), new SqlParameter() };
            //sqlparams[0] = new SqlParameter("@inputone", SqlDbType.Int);
            //sqlparams[0].Value = Convert.ToInt32(80);
            //sqlparams[1] = new SqlParameter("@inputtwo", SqlDbType.TinyInt);
            //sqlparams[1].Value = Convert.ToInt16(80);
            //sqlparams[2] = new SqlParameter("@inputthree", SqlDbType.SmallInt);
            //sqlparams[2].Value = Convert.ToInt16(80);

        }
        #endregion

        #region Funciones publicas
        ///ejecuta Querys
        public DataTable Execute_Query()
        {
            DataTable tabla = new DataTable();
            Build_Command();
            try
            {
                Start_Connection();
                _adapter = new SqlDataAdapter(_command);
                _adapter.Fill(tabla);
                Stop_Connection();
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
            }
            return tabla;
        }
        public DataTable Execute_Query(string Query)
        {
            _script = Query;
            DataTable tabla = new DataTable();
            Build_Command();
            try
            {
                Start_Connection();
                _adapter = new SqlDataAdapter(_command);
                _adapter.Fill(tabla);
                Stop_Connection();
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
            }
            return tabla;
        }
        //public DataTable Execute_Query(string Query,SqlParameter[] Parameters)
        //{   _script = Query;
        //    DataTable tabla = new DataTable();
        //    Build_Command();
        //    try
        //    {
        //        Start_Connection();
        //        _adapter = new SqlDataAdapter(_command);
        //        _adapter.Fill(tabla);
        //        Stop_Connection();
        //    }
        //    catch (Exception ex)
        //    {
        //        _error_mjs = ex.Message;
        //        Stop_Connection();
        //    }
        //    return tabla;
        //}
        #endregion
    }
    public class Data_path
    {
        /////CAMPOS////////
        private string App_path = "";
        private string DB_path = "";
        private string IMG_path = "";
        private string LOG_path = "";
        private string RPT_path = "";
        private string CFG_path = "";
        private string UPDATE_path = "";
        private string BKUP_path = "";

        //////////////////////

        /////PROPIEDADES//////
        #region PROPIEDADES
        public string DATA_BASE
        {
            get
            {
                return DB_path;
            }
            set
            {
                if (value != DB_path) DB_path = value;
            }
        }
        public string IMAGES
        {
            get
            {
                return IMG_path;
            }
            set
            {
                if (value != IMG_path) IMG_path = value;
            }
        }
        public string LOG
        {
            get
            {
                return LOG_path;
            }
            set
            {
                if (value != LOG_path) LOG_path = value;
            }
        }
        public string REPORTS
        {
            get
            {
                return RPT_path;
            }
            set
            {
                if (value != RPT_path) RPT_path = value;
            }
        }
        public string APPLICATION
        {
            get
            {
                return App_path;
            }
            set
            {
                if (value != App_path) App_path = value;
            }
        }
        public string CONFIGURATION
        {
            get
            {
                return CFG_path;
            }
            set
            {
                if (value != CFG_path) CFG_path = value;
            }
        }
        public string UPDATE
        {
            get
            {
                return UPDATE_path;
            }
            set
            {
                if (value != UPDATE_path) UPDATE_path = value;
            }
        }
        public string BACKUPS
        {
            get
            {
                return BKUP_path;
            }
            set
            {
                if (value != BKUP_path) BKUP_path = value;
            }
        }
        #endregion
        ///////////////////////

        /////CONSTRUCTOR///////
        public Data_path()
        {
            try
            {
                FileStream archivo = new FileStream("data_path", FileMode.Open, FileAccess.Read);
                StreamReader entrada = new StreamReader(archivo);
                DATA_BASE = entrada.ReadLine();
                IMAGES = entrada.ReadLine();
                LOG = entrada.ReadLine();
                REPORTS = entrada.ReadLine();
                CONFIGURATION = entrada.ReadLine();
                BACKUPS = entrada.ReadLine();
                UPDATE = entrada.ReadLine();
                APPLICATION = entrada.ReadLine();
                entrada.Close();
            }
            catch (Exception ex)
            {
                string message = ex.Message;
            }
        }
        ///////////////////////

        /////FUNCIONES/////////
        public void load_path()
        {
            try
            {
                FileStream archivo = new FileStream("data_path", FileMode.Open, FileAccess.Read);
                StreamReader entrada = new StreamReader(archivo);
                DATA_BASE = entrada.ReadLine();
                IMAGES = entrada.ReadLine();
                LOG = entrada.ReadLine();
                REPORTS = entrada.ReadLine();
                CONFIGURATION = entrada.ReadLine();
                BACKUPS = entrada.ReadLine();
                APPLICATION = entrada.ReadLine();
                entrada.Close();
            }
            catch (Exception ex)
            {
                string message = ex.Message;
            }
        }
        ///////////////////////
    }
    public class Query_builder
    {
        private string Table_Name = "";
        private List<string> Fields = new List<string>();
        private List<string> Values = new List<string>();
    
        public enum Query_Type
        {
            UPDATE, INSERT, SELECT, DELETE
        }
        public Query_builder()
        { }
    }
    public class criptografia
    {
        int llave = 0;
        //constructor
        public criptografia()
        { }
        //cargar llave recive la llave en string
        public void cargar_llave(string key)
        {
            if (key == "")
                llave = 0;
            else
            {
                llave = 0;
                for (int M = 0; M < key.Length; M++)
                llave = llave + (int)Convert.ToChar(key.Substring(M, 1));
            }
        }
        //encritar de palabra recive un string entrega string
        public String EncriptarPalabra(String PalabraAEncriptar)
        {
            String PalabraEncriptada;
            PalabraEncriptada = "";
            int EnteroTemporal;

            for (int M = 0; M < PalabraAEncriptar.Length; M++)
            {
                EnteroTemporal = (int)Convert.ToChar(PalabraAEncriptar.Substring(M, 1)) + llave;
                PalabraEncriptada = PalabraEncriptada + (char)EnteroTemporal;
            }

            return PalabraEncriptada;
        }
        //desencriptar de palabra  un string entrega string
        public String DesencriptarPalabra(String PalabraADesencriptar)
        {
            String PalabraDesencriptada;
            PalabraDesencriptada = "";
            int EnteroTemporal;
            for (int M = 0; M < PalabraADesencriptar.Length; M++)
            {
                EnteroTemporal = (int)Convert.ToChar(PalabraADesencriptar.Substring(M, 1)) - llave;
                PalabraDesencriptada = PalabraDesencriptada + (char)EnteroTemporal;
            }

            return PalabraDesencriptada;
        }
    }
}
namespace Data_Base_MNG
{
    public class Access
    {
        private string Command = "";
        private string DB_file = "";
        private string archivodb = "";
        private OleDbConnection conector;
        private OleDbCommand OleCommand;
        private string exeption = "";
        private bool error = false;

        ///Propiedades/////////////////////////////////////////////////
        #region Propiedades
        public string Command_Query
        {
            get
            {
                return Command;
            }
            set
            {
                if (value != Command) Command = value;
            }
        }
        public string DB_File
        {
            get
            {
                return DB_file;
            }
            set
            {
                if (value != DB_file) DB_file = value;
            }
        }
        public string Connection_String
        {
            get
            {
                return archivodb;
            }
            set
            {
                if (value != archivodb) archivodb = value;
            }
        }
        public string Exeption
        {
            get
            {
                return exeption;
            }
            //set
            //{
            //    if (value != exeption) exeption = value;
            //}
        }
        public bool Error
        {
            get
            {
                return error;
            }
            //set
            //{
            //    throw new System.NotImplementedException();
            //}
        }
        #endregion

        ///Costructor//////////////////////////////////////////////////
        #region Costructores
        public Access(string Data_Base)
        {
            DB_file = Data_Base;
            conector = new OleDbConnection(Build_ConStr());
            error = false;
        }
        public Access(string Data_Base, string Query)
        {
            Command = Query;
            DB_file = Data_Base;
            conector = new OleDbConnection(Build_ConStr());
            error = false;
        }
        #endregion

        ///Funciones///////////////////////////////////////////////////
        #region Funciones
        private string Build_ConStr()///construye el Connection String
        {
            string conn07 = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=";
            string conn00 = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=";
            string connection = DB_file;
            if (connection.Contains(".accdb")) connection = conn07 + connection;
            if (connection.Contains(".mdb")) connection = conn00 + connection;
            archivodb = connection;
            //string archivodb = ("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + DB_file);
            return archivodb;
        }
        private void Build_Comm()/// construye OLE COMMAND
        {
            OleCommand = new OleDbCommand(Command, conector);
        }
        ///USER FUNCIONS
        public void Load_Command(string DB_Command)///Carga el comando SQL
        {
            Command = DB_Command;
            Build_Comm();
        }
        public bool Execute_Command()///Ejecuta Commando que no sea Query
        {
            exeption = "";
            error = false;
            try
            {
                conector.Open();
                OleCommand.ExecuteNonQuery();
                conector.Close();
                return true;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return false;
            }
        }
        public bool Execute_Command(string DB_command)///Ejecuta Commando que no sea Query
        {
            exeption = "";
            Command = DB_command;
            Build_Comm();
            error = false;
            try
            {
                conector.Open();
                OleCommand.ExecuteNonQuery();
                conector.Close();
                return true;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return false;
            }
        }
        public DataTable Execute_Query()///Ejecuta Query y regresa una tabla llena
        {
            exeption = "";
            DataTable tabla_Query = new DataTable();
            error = false;
            try
            {
                conector.Open();
                OleDbDataAdapter adaptador = new OleDbDataAdapter(Command, conector);
                OleDbCommandBuilder actualizardb = new OleDbCommandBuilder(adaptador);
                adaptador.Fill(tabla_Query);
                conector.Close();
                return tabla_Query;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                error = true;
                conector.Close();
                return tabla_Query;
            }
        }
        public DataTable Execute_Query(string Query)///Ejecuta Query y regresa una tabla llena
        {
            exeption = "";
            Command = Query;
            Build_Comm();
            error = false;
            DataTable tabla_Query = new DataTable();
            try
            {
                conector.Open();
                OleDbDataAdapter adaptador = new OleDbDataAdapter(Command, conector);
                OleDbCommandBuilder actualizardb = new OleDbCommandBuilder(adaptador);
                adaptador.Fill(tabla_Query);
                conector.Close();
                return tabla_Query;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return tabla_Query;
            }
        }
        public bool Execute_Query(out DataTable Table2Fill)///Ejecuta Query y regresa una tabla llena
        {
            exeption = "";
            Build_Comm();
            error = false;
            Table2Fill = new DataTable();
            try
            {
                conector.Open();
                OleDbDataAdapter adaptador = new OleDbDataAdapter(Command, conector);
                OleDbCommandBuilder actualizardb = new OleDbCommandBuilder(adaptador);
                adaptador.Fill(Table2Fill);
                conector.Close();
                return true;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return false;
            }
        }
        public bool Execute_Query(string Query, out DataTable Table2Fill)///Ejecuta Query y regresa una tabla llena
        {
            exeption = "";
            Command = Query;
            Build_Comm();
            error = false;
            Table2Fill = new DataTable();
            try
            {
                conector.Open();
                OleDbDataAdapter adaptador = new OleDbDataAdapter(Command, conector);
                OleDbCommandBuilder actualizardb = new OleDbCommandBuilder(adaptador);
                adaptador.Fill(Table2Fill);
                conector.Close();
                return true;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return false;
            }
        }
        public bool Execute_Query(out DataSet DataSet2Fill)///Ejecuta Query y regresa una tabla llena
        {
            exeption = "";
            Build_Comm();
            error = false;
            DataSet2Fill = new DataSet();
            try
            {
                conector.Open();
                OleDbDataAdapter adaptador = new OleDbDataAdapter(Command, conector);
                OleDbCommandBuilder actualizardb = new OleDbCommandBuilder(adaptador);
                adaptador.Fill(DataSet2Fill);
                conector.Close();
                return true;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return false;
            }
        }
        public bool Execute_Query(string Query, out DataSet DataSet2Fill)///Ejecuta Query y regresa una tabla llena
        {
            exeption = "";
            Command = Query;
            Build_Comm();
            error = false;
            DataSet2Fill = new DataSet();
            try
            {
                conector.Open();
                OleDbDataAdapter adaptador = new OleDbDataAdapter(Command, conector);
                OleDbCommandBuilder actualizardb = new OleDbCommandBuilder(adaptador);
                adaptador.Fill(DataSet2Fill);
                conector.Close();
                return true;
            }
            catch (Exception ex)
            {
                exeption = ex.Message;
                conector.Close();
                error = true;
                return false;
            }
        }
        public DataTable Schematic_of_Tables()
        {
            DataTable Schematictable = new DataTable();
            conector.Open();
            Schematictable = conector.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
            conector.Close();
            return Schematictable;
        }///entrega las tablas que existen en la BD
        public DataTable Schematic_of_Columns(string DB_table)
        {
            DataTable Schematictable = new DataTable();
            conector.Open();
            Schematictable = conector.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, DB_table, null });
            conector.Close();
            return Schematictable;
        }///entrega los campos que existen en la BD
        #endregion
    }
    public class SQL
    {
        //sql_con = "Data Source=" + server + ";Initial Catalog=FSDBMR;Persist Security Info=True;User ID=" + user + ";Password=" + pass;
        #region Variables Privadas
        private string _server = "";
        private string _user = "";
        private string _pass = "";
        private string _dbname = "";
        private string _sql_con_str = "";
        private SqlConnection _connection;
        private SqlTransaction _transaction;

        private List<SqlParameter> _parameters = new List<SqlParameter>();
		private List<SqlParameter> _parametersOutput = new List<SqlParameter>();

        //private SqlParameter[] _parameters;
        private SqlCommand _command;
        private string _script = "";
        private string _error_mjs = "";
        private SqlDataAdapter _adapter;
        private bool ErrorFlag = false;
        #endregion

        #region Variales Publicas
        //public SqlDataAdapter Open_adapter;
        public string Command
        {
            get
            {
                return _script;
            }
            set
            {
                if (value != _script) _script = value;
            }
        }
        public string Server
        {
            get
            {
                return _server;
            }
            set
            {
                if (value != _server) _server = value;
            }
        }
        public string DB_Name
        {
            get
            {
                return _dbname;
            }
            set
            {
                if (value != _dbname) _dbname = value;
            }
        }
        public string User
        {
            get
            {
                return _user;
            }
            set
            {
                if (value != _user) _user = value;
            }
        }
        public string Pass
        {
            get
            {
                return _pass;
            }
            set
            {
                if (value != _pass) _pass = value;
            }
        }
        public string Error_Mjs
        {
            get
            {
                return _error_mjs;
            }
            set
            {
                //if (value != _error_mjs) _error_mjs = value;
            }
        }
        public bool ErrorOccur
        {
            get
            {
                return ErrorFlag;
            }
            set
            {
            } 
        }
        #endregion

        #region Constructores
        public SQL(string SQL_Connection_String)
        {
            _sql_con_str = SQL_Connection_String;
            _connection = new SqlConnection(_sql_con_str);
        }
        public SQL(string Server, string DBname, string User, string Pass)
        {
            _sql_con_str = "Data Source=" + Server + ";Initial Catalog=" + DBname +
                ";Persist Security Info=True;User ID=" + User + ";Password=" + Pass;
            _connection = new SqlConnection(_sql_con_str);
        }
        #endregion

        #region Funciones Privadas
        private void Start_Connection()
        {
            if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();
            }
        }
        private void Start_Connection_BeginTransaction(string TransactionName)
        {
            if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();
                _transaction = _connection.BeginTransaction(TransactionName);
            }
        }
        private void Commit()
        {
            try
            {
                ErrorFlag = false;
                _error_mjs = "";
                _transaction.Commit();
            }
            catch (Exception ex)
            {
                ErrorFlag = true;
                _error_mjs = ex.Message;
                try
                {
                    _transaction.Rollback();
                }
                catch (Exception ex2)
                {
                    _error_mjs = ex.Message + " RollBack Error: " + ex2.Message;
                }
            } 
        }

        public void RollBack()
        {
            ErrorFlag = false;
            _error_mjs = "";
            try
            {
                _transaction.Rollback();
            }
            catch (Exception ex)
            {
                ErrorFlag = true;
                _error_mjs = ex.Message;
            }
        }
        private void Stop_Connection()
        {
            if (_connection.State == ConnectionState.Open)
            {
                _connection.Close();
            }
        }
        private void Build_Adapter()
        {
            _adapter = new SqlDataAdapter(_script, _connection);
        }
        private void Build_Command()
        {
            _command = new SqlCommand(_script, _connection);
        }
        private void Build_Command(string Command)
        {
            _script = Command;
            _command = new SqlCommand(_script, _connection);
        }
        private object Execute_Scalar_Object(string Command)
        {
            object result = null;
            Build_Command(Command);
            try
            {
                Start_Connection();
                result = _command.ExecuteScalar();
                Stop_Connection();
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                result = null;
            }
            return result;
        }
        private object Execute_Scalar_Object_OpenConnection(string Command)
        {
            object result = null;
            Build_Command(Command);
            try
            {
                //Start_Connection();
                result = _command.ExecuteScalar();
                Stop_Connection();
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                //Stop_Connection();
                result = null;
            }
            return result;
        }
        #endregion

        #region Funciones publicas
        ///ejecuta Querys
        public void Close_Open_Connection()
        {
            Stop_Connection();
        }
        public void Open_Connection()
        {
            Start_Connection();
        }
        public void Open_Connection(string TransactionName)
        {
            Start_Connection_BeginTransaction(TransactionName);
        }
        public void CommitTransaction()
        {
            Commit();
        }
        public bool Execute_Command()
        {
            bool result = false;
            Build_Command();
            try
            {
                Start_Connection();
                ///
                _command.ExecuteNonQuery();
                Stop_Connection();
                ErrorFlag = false;
                result = true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                result = false;
            }
            return result;
        }
        public bool Execute_Command(string Command)
        {
            bool result = false;
            Build_Command(Command);
            try
            {
                Start_Connection();
                ///
                _command.ExecuteNonQuery();
                Stop_Connection();
                ErrorFlag = false;
                result = true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                result = false;
            }
            return result;
        }
        public bool Execute_Command_Open_Connection(string Command)
        {
            Build_Command(Command);
            try
            {
				_command.Transaction = _transaction;
                _command.ExecuteNonQuery();
                //Stop_Connection();
                ErrorFlag = false;
                return true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;                
                ErrorFlag = true;

                try
                {
                    _transaction.Rollback();
                }
                catch (Exception ex2)
                {
                    _error_mjs = ex.Message + " RollBack Error: " + ex2.Message;
                }
            }
            return false;
        }
        public DataTable Execute_Query()
        {
            DataTable tabla = new DataTable();
            Build_Command();
            try
            {
                Start_Connection();
                _adapter = new SqlDataAdapter(_command);
                _adapter.Fill(tabla);
                Stop_Connection();
                ErrorFlag = false;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
            }
            return tabla;
        }
        public DataTable Execute_Query(string Query)
        {
            DataTable tabla = new DataTable();
            Build_Command(Query);
            try
            {
                Start_Connection();
                _adapter = new SqlDataAdapter(_command);
                _adapter.Fill(tabla);
                Stop_Connection();
                ErrorFlag = false;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
            }
            return tabla;
        }
        public bool Execute_Query(string Query, out DataTable Table2Fill)
        {
            Table2Fill = new DataTable();
            Build_Command(Query);
            try
            {
                Start_Connection();
                _adapter = new SqlDataAdapter(_command);
                _adapter.Fill(Table2Fill);
                Stop_Connection();
                ErrorFlag = false;
                return true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                return false;
            }
        }
        public bool Execute_Query(out DataTable Table2Fill)
        {
            Table2Fill = new DataTable();
            Build_Command();
            try
            {
                Start_Connection();
                _adapter = new SqlDataAdapter(_command);
                _adapter.Fill(Table2Fill);
                Stop_Connection();
                ErrorFlag = false;
                return true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                return false;
            }
        }
        public bool Execute_Query_Open_Connection(string Query, out DataTable Table2Fill)
        {
            Table2Fill = new DataTable();
            Build_Command(Query);
            try
            {
                //Start_Connection();
                _adapter = new SqlDataAdapter(_command);
                _adapter.Fill(Table2Fill);
                //Stop_Connection();
                ErrorFlag = false;
                return true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                return false;
            }
        }
        public bool Execute_Query_Open_Connection(out DataTable Table2Fill)
        {
            Table2Fill = new DataTable();
            Build_Command();
            try
            {
                //Start_Connection();
                _adapter = new SqlDataAdapter(_command);
                _adapter.Fill(Table2Fill);
                //Stop_Connection();
                ErrorFlag = false;
                return true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                return false;
            }
        }
        public bool Execute_Table_Update(out DataTable Table2Fill)
        {
            Table2Fill = new DataTable();
            try
            {
                _adapter.Update(Table2Fill);

                ErrorFlag = false;
                return true;
            }
            catch (Exception ex)
            {
                Error_Mjs = ex.Message;
                ErrorFlag = true;
                return false;
            }
        }
        public void Load_SP_Parameters(string ParameterName, string ParameterValue)
        {
            SqlParameter param = new SqlParameter(ParameterName, ParameterValue);
            _parameters.Add(param);
        }
        public void Load_SP_Parameters_Output(string ParameterName, SqlDbType type, int length)
        {
            SqlParameter param = new SqlParameter(ParameterName, type,length);
            param.Direction = ParameterDirection.Output;
            _parametersOutput.Add(param);
        }
        public void Load_SP_Parameters_Output(string ParameterName, SqlDbType type)
        {
            SqlParameter param = new SqlParameter(ParameterName, type);
            param.Direction = ParameterDirection.Output;
            _parametersOutput.Add(param);
        }
        public string getOutputParameter(string param)
        {
            return _command.Parameters[param].Value.ToString();
        }
        public void clearOutputParameters()
        {
            _parametersOutput.Clear();
        }
        public bool Execute_StoreProcedure(string Command, bool ParametersNeeded)
        {
            bool result = false;
            Build_Command(Command);
            _command.CommandType = CommandType.StoredProcedure;
            try
            {
                Start_Connection();
                ///

                if (ParametersNeeded)
                {

                    _command.Parameters.AddRange(_parameters.ToArray());
                }
                _command.ExecuteNonQuery();
                Stop_Connection();
                ErrorFlag = false;
                result = true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                result = false;
            }
            return result;
        }
        public string Execute_StoreProcedure_Scalar(string Command, bool ParametersNeeded)
        {
            string result = "";
            Build_Command(Command);
            _command.CommandType = CommandType.StoredProcedure;
            try
            {
                Start_Connection();
                ///

                if (ParametersNeeded)
                {

                    _command.Parameters.AddRange(_parameters.ToArray());
                }
                result = _command.ExecuteScalar().ToString();
                Stop_Connection();
                ErrorFlag = false;
                //result = true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                result = "";
            }
            return result;
        }
        public DataTable Execute_StoreProcedure_Table(string Command, bool ParametersNeeded)
        {
            DataTable result = new DataTable();
            Build_Command(Command);
            _command.CommandType = CommandType.StoredProcedure;
            
            try
            {
                Start_Connection();
                ///

                if (ParametersNeeded)
                {

                    _command.Parameters.AddRange(_parameters.ToArray());
                }
                _adapter = new SqlDataAdapter(_command);

                _adapter.Fill(result);
                Stop_Connection();
                ErrorFlag = false;
                //result = true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                Stop_Connection();
                ErrorFlag = true;
                result = null;
            }
            return result;
        }
        public bool Execute_StoreProcedure_Open_Conn(string Command, bool ParametersNeeded)
        {
            bool result = false;
            Build_Command(Command);
            _command.CommandType = CommandType.StoredProcedure;
            try
            {
                //Start_Connection();
                ///

                if (ParametersNeeded)
                {

                    _command.Parameters.AddRange(_parameters.ToArray());
                }
                _command.Transaction = _transaction;
                _command.ExecuteNonQuery();                
                _command.Parameters.Clear();
                _parameters.Clear();
               
                //Stop_Connection();
                ErrorFlag = false;
                result = true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                //Stop_Connection();
                ErrorFlag = true;
                result = false;
            }
            return result;
        }
        public string Execute_StoreProcedure_Scalar_Open_Conn(string Command, bool ParametersNeeded)
        {
            string result = "";
            Build_Command(Command);
            _command.CommandType = CommandType.StoredProcedure;
            try
            {
                //Start_Connection();
                ///

                if (ParametersNeeded)


                {                   
                    _command.Parameters.AddRange(_parameters.ToArray());
                }
                _command.Transaction = _transaction;
                result = _command.ExecuteScalar().ToString();
                _command.Parameters.Clear();
                _parameters.Clear();
                //Stop_Connection();
                ErrorFlag = false;
                //result = true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                //Stop_Connection();
                ErrorFlag = true;
                result = "";

                try
                {
                    _transaction.Rollback();
                }
                catch (Exception ex2)
                {
                    _error_mjs = ex.Message + " RollBack Error: " + ex2.Message;
                }
            }
            return result;
        }
public bool Execute_StoreProcedure_Use_Output_Parameters_Open_Conn(string Command, bool ParametersNeeded)
        {
            bool result = false;
            Build_Command(Command);
            _command.CommandType = CommandType.StoredProcedure;
            try
            {
                //Start_Connection();
                ///
                _command.Parameters.Clear();
                if (ParametersNeeded)
                {
                    _command.Parameters.AddRange(_parameters.ToArray());
                    _command.Parameters.AddRange(_parametersOutput.ToArray());
                }
                _command.Transaction = _transaction;
                _command.ExecuteNonQuery();
                
                _parameters.Clear();
                //Stop_Connection();
                ErrorFlag = false;
                result = true;
            }
            catch (Exception ex)
            {

                _error_mjs = ex.Message;
                //Stop_Connection();
                ErrorFlag = true;
                result = false;

                try
                {
                    _transaction.Rollback();
                }
                catch (Exception ex2)
                {
                    _error_mjs = ex.Message + " RollBack Error: " + ex2.Message;
                }
            }
            return result;
        }
        public DataTable Execute_StoreProcedure_Table_Open_Conn(string Command, bool ParametersNeeded)
        {
            DataTable result = new DataTable();
            Build_Command(Command);
            _command.CommandType = CommandType.StoredProcedure;

            try
            {
                //Start_Connection();
                ///

                if (ParametersNeeded)
                {

                    _command.Parameters.AddRange(_parameters.ToArray());
                }
                _adapter = new SqlDataAdapter(_command);

                _adapter.Fill(result);
                //Stop_Connection();
                ErrorFlag = false;
                //result = true;
            }
            catch (Exception ex)
            {
                _error_mjs = ex.Message;
                //Stop_Connection();
                ErrorFlag = true;
                result = null;
            }
            return result;
        }

        public bool Update_Open_Connection( out DataTable Table2Update)
        {
            Table2Update = new DataTable();       
            try
            {
                _adapter.Update(Table2Update);
                ErrorFlag = false;
                return true;
            }
            catch(Exception ex)
            {
                _error_mjs = ex.Message;
                ErrorFlag = true;
                return false;
            }
        }
        public string Execute_Scalar(string Query)
        {
            object objeto = null;
            string Dato = "";
            try
            {
                objeto = Execute_Scalar_Object(Query);
                Dato = objeto.ToString();
                ErrorFlag = false;
                return Dato;
            }
            catch (Exception ex)
            {
                Error_Mjs = ex.Message;
                ErrorFlag = true;
                return "";
            }
        }
        public string Execute_Scalar_Open_Conn(string Query)
        {
            object objeto = null;
            string Dato = "";
            try
            {
                objeto = Execute_Scalar_Object_OpenConnection(Query);
                Dato = objeto.ToString();
                ErrorFlag = false;
                return Dato;
            }
            catch (Exception ex)
            {
                Error_Mjs = ex.Message;
                ErrorFlag = true;
                return "";
            }
        }
        #endregion
    }
}
namespace PANDA_TOOLS
{
    public class CDF_to_DataTable
    {
        #region Variables Globales
        string File_Path = "";
        public string error_msg = "";
        public bool error = false;
        #endregion

        #region Variables Locales
        private bool First_Row_Column_Names_Flag = false;
        private DataTable file_table = null;
        private string[] header = null;
        private char Delimeter = ',';
        #endregion

        #region Constructores
        public CDF_to_DataTable(char Delimited_Char, string FilePath, bool First_Row_Column_Names)
        {
            Delimeter = Delimited_Char;
            File_Path = FilePath;
            First_Row_Column_Names_Flag = First_Row_Column_Names;
        }
        #endregion

        #region Funciones Privadas
        private DataTable FlatText_to_DataTable()
        {
            string line = "";
            int count = 0;
            int x = 0;
            file_table = new DataTable();
            string[] row;
            try
            {
                FileStream archivo = new FileStream(File_Path, FileMode.Open, FileAccess.Read);
                StreamReader entrada = new StreamReader(archivo);
                if (First_Row_Column_Names_Flag)
                {
                    line = entrada.ReadLine();
                    header = line.Split(Delimeter);
                    for (int i = 0; i < header.Count(); i++)
                    {
                        file_table.Columns.Add(header[i].ToString(), typeof(string));
                    }
                }
                else
                {
                    line = entrada.ReadLine();
                    row = line.Split('|');
                    for (int i = 0; i < row.Count(); i++)
                    {
                        file_table.Columns.Add("Column" + i.ToString(), typeof(string));
                    }
                    file_table.Rows.Add(row);
                }
                while (x == 0)
                {
                    line = entrada.ReadLine();
                    if (line != null)
                    {
                        count++;
                        row = line.Split(Delimeter);
                        file_table.Rows.Add(row);
                    }
                    else
                    {
                        x = 1;
                    }
                }
                error = false;
                entrada.Close();
                return file_table;
            }
            catch (Exception ex)
            {
                error = true;
                error_msg = ex.Message;
                return null;
            }
        }//Reads Plain text file and converts it in to a datatable
        #endregion

        #region Funciones Publicas 
        public DataTable Start_Conversion()
        {
            DataTable Table = null;
            Table = FlatText_to_DataTable();
            return Table;
        }
        #endregion

    }
}
namespace TOOLS
{
    public class criptografia
    {
        int llave = 0;
        //constructor
        public criptografia()
        { }
        //cargar llave recive la llave en string
        public void cargar_llave(string key)
        {
            if (key == "")
                llave = 0;
            else
            {
                llave = 0;
                for (int M = 0; M < key.Length; M++)
                    llave = llave + (int)Convert.ToChar(key.Substring(M, 1));
            }
        }
        //encritar de palabra recive un string entrega string
        public String EncriptarPalabra(String PalabraAEncriptar)
        {
            String PalabraEncriptada;
            PalabraEncriptada = "";
            int EnteroTemporal;

            for (int M = 0; M < PalabraAEncriptar.Length; M++)
            {
                EnteroTemporal = (int)Convert.ToChar(PalabraAEncriptar.Substring(M, 1)) + llave;
                PalabraEncriptada = PalabraEncriptada + (char)EnteroTemporal;
            }

            return PalabraEncriptada;
        }
        //desencriptar de palabra  un string entrega string
        public String DesencriptarPalabra(String PalabraADesencriptar)
        {
            String PalabraDesencriptada;
            PalabraDesencriptada = "";
            int EnteroTemporal;
            for (int M = 0; M < PalabraADesencriptar.Length; M++)
            {
                EnteroTemporal = (int)Convert.ToChar(PalabraADesencriptar.Substring(M, 1)) - llave;
                PalabraDesencriptada = PalabraDesencriptada + (char)EnteroTemporal;
            }

            return PalabraDesencriptada;
        }
    }
    public class Email
    {
        /*
         * Cliente SMTP
         * Gmail:  smtp.gmail.com  puerto:587
         * Hotmail: smtp.liva.com  puerto:25
         */
        MailMessage mnsj = new MailMessage();
        SmtpClient server = new SmtpClient("smtp.gmail.com", 587);
        string EmailFrom = "capsonic.apps@gmail.com";
        string Password = "cApsOnIc13";

        public Email()
        {
            /*
             * Autenticacion en el Servidor
             * Utilizaremos nuestra cuenta de correo
             *
             * Direccion de Correo (Gmail o Hotmail)
             * y Contrasena correspondiente
             */
            //aaron.corrales.zt@gmail.com

            server.Credentials = new System.Net.NetworkCredential(EmailFrom, Password);
            server.EnableSsl = true;
        }

        private void _SendEmail(MailMessage Message)
        {
            server.Send(Message);
        }

        public void CreateMessage(string To, string Subject, string Body)
        {
            string[] ToAdresses = To.Split(',');
            for (int i = 0; i < ToAdresses.Count(); i++)
            {
                mnsj.To.Add(new MailAddress(ToAdresses[i])); 
            }
            mnsj.From = new MailAddress(EmailFrom, EmailFrom);
            mnsj.Subject = Subject;
            mnsj.Body = Body;

        }
        public void SendEmail()
        {
            _SendEmail(mnsj);
        }
        public void CreateMessageHTML(string To, string Subject, AlternateView Body)
        {
            mnsj.To.Add(new MailAddress(To));
            mnsj.From = new MailAddress(EmailFrom, EmailFrom);
            mnsj.Subject = Subject;
            mnsj.IsBodyHtml = true;
            mnsj.BodyEncoding = System.Text.Encoding.UTF8;
            mnsj.AlternateViews.Add(Body);

        }
    }
    public class Dataloger
    {
        #region Private Variables 
        private string _FileName = "";
        private string _Extention = "";
        private string _FilePath = "";
        private string _Error = "";
        private string _Separator = "|";
        #endregion

        #region Public Variables
        public enum Category
        {
            Info,
            Warning,
            Error
        };
        #endregion

        #region Constructors
        public Dataloger(string LogFileName, string LogFileExtention,string LogFilePath)
        {
            _FileName = LogFileName;
            _Extention = LogFileExtention;
            _FilePath = LogFilePath;
        }
        #endregion

        #region Public Functions
        public bool WriteLogLine(Category LogCat,string Line)
        {
            string file = _FilePath + _FileName + "." + _Extention;
            _Error = "";
            try
            {
                FileStream fileStream = new FileStream(file, FileMode.Append, FileAccess.Write);
                StreamWriter writer = new StreamWriter(fileStream);
                writer.WriteLine(DateTime.Now.ToString("MM/dd/yy hh:mm:ss") + _Separator + LogCat.ToString() + _Separator + Line);
                writer.Close();
                fileStream.Close();
                return true;
            }
            catch(Exception ex)
            {                
                _Error = ex.Message;
                return false;
            }
        }
        public DataTable LoadLogFile()
        {
            DataTable table = new DataTable();

            #region Open_file
            string file = _FilePath + _FileName + "." + _Extention;
           
            List<string> Lines = new List<string>();

            FileStream fileStream = new FileStream(file, FileMode.Open, FileAccess.Read);

            StreamReader reader = new StreamReader(fileStream);

            while (!reader.EndOfStream)
            {
                Lines.Add(reader.ReadLine());
            }

            fileStream.Close();

            #endregion

            //Lines[0];
            
            table.Columns.Add("Date", typeof(DateTime));
            table.Columns.Add("Category", typeof(string));
            table.Columns.Add("Log", typeof(string));

            for (int j = 1; j < Lines.Count(); j++)
            {
                string TableRow = Lines[j].Replace("\"", "");
                string[] TableData = TableRow.Split('|');
                table.Rows.Add(TableData);
            }
            return table;
        }
        #endregion

    }
    public class INIFile
    {

        #region "Declarations"

        // *** Lock for thread-safe access to file and local cache ***
        private object m_Lock = new object();

        // *** File name ***
        private string m_FileName = null;
        public string FileName
        {
            get
            {
                return m_FileName;
            }
        }

        // *** Lazy loading flag ***
        private bool m_Lazy = false;

        // *** Automatic flushing flag ***
        private bool m_AutoFlush = false;

        // *** Local cache ***
        private Dictionary<string, Dictionary<string, string>> m_Sections = new Dictionary<string, Dictionary<string, string>>();
        private Dictionary<string, Dictionary<string, string>> m_Modified = new Dictionary<string, Dictionary<string, string>>();

        // *** Local cache modified flag ***
        private bool m_CacheModified = false;

        #endregion

        #region "Methods"

        // *** Constructor ***
        public INIFile(string FileName)
        {
            Initialize(FileName, false, false);
        }

        public INIFile(string FileName, bool Lazy, bool AutoFlush)
        {
            Initialize(FileName, Lazy, AutoFlush);
        }

        // *** Initialization ***
        private void Initialize(string FileName, bool Lazy, bool AutoFlush)
        {
            m_FileName = FileName;
            m_Lazy = Lazy;
            m_AutoFlush = AutoFlush;
            if (!m_Lazy) Refresh();
        }

        // *** Parse section name ***
        private string ParseSectionName(string Line)
        {
            if (!Line.StartsWith("[")) return null;
            if (!Line.EndsWith("]")) return null;
            if (Line.Length < 3) return null;
            return Line.Substring(1, Line.Length - 2);
        }

        // *** Parse key+value pair ***
        private bool ParseKeyValuePair(string Line, ref string Key, ref string Value)
        {
            // *** Check for key+value pair ***
            int i;
            if ((i = Line.IndexOf('=')) <= 0) return false;

            int j = Line.Length - i - 1;
            Key = Line.Substring(0, i).Trim();
            if (Key.Length <= 0) return false;

            Value = (j > 0) ? (Line.Substring(i + 1, j).Trim()) : ("");
            return true;
        }

        // *** Read file contents into local cache ***
        public void Refresh()
        {
            lock (m_Lock)
            {
                StreamReader sr = null;
                try
                {
                    // *** Clear local cache ***
                    m_Sections.Clear();
                    m_Modified.Clear();

                    // *** Open the INI file ***
                    try
                    {
                        sr = new StreamReader(m_FileName);
                    }
                    catch (FileNotFoundException)
                    {
                        return;
                    }

                    // *** Read up the file content ***
                    Dictionary<string, string> CurrentSection = null;
                    string s;
                    string SectionName;
                    string Key = null;
                    string Value = null;
                    while ((s = sr.ReadLine()) != null)
                    {
                        s = s.Trim();

                        // *** Check for section names ***
                        SectionName = ParseSectionName(s);
                        if (SectionName != null)
                        {
                            // *** Only first occurrence of a section is loaded ***
                            if (m_Sections.ContainsKey(SectionName))
                            {
                                CurrentSection = null;
                            }
                            else
                            {
                                CurrentSection = new Dictionary<string, string>();
                                m_Sections.Add(SectionName, CurrentSection);
                            }
                        }
                        else if (CurrentSection != null)
                        {
                            // *** Check for key+value pair ***
                            if (ParseKeyValuePair(s, ref Key, ref Value))
                            {
                                // *** Only first occurrence of a key is loaded ***
                                if (!CurrentSection.ContainsKey(Key))
                                {
                                    CurrentSection.Add(Key, Value);
                                }
                            }
                        }
                    }
                }
                finally
                {
                    // *** Cleanup: close file ***
                    if (sr != null) sr.Close();
                    sr = null;
                }
            }
        }

        // *** Flush local cache content ***
        public void Flush()
        {
            lock (m_Lock)
            {
                PerformFlush();
            }
        }

        private void PerformFlush()
        {
            // *** If local cache was not modified, exit ***
            if (!m_CacheModified) return;
            m_CacheModified = false;

            // *** Check if original file exists ***
            bool OriginalFileExists = File.Exists(m_FileName);

            // *** Get temporary file name ***
            string TmpFileName = Path.ChangeExtension(m_FileName, "$n$");

            // *** Copy content of original file to temporary file, replace modified values ***
            StreamWriter sw = null;

            // *** Create the temporary file ***
            sw = new StreamWriter(TmpFileName);

            try
            {
                Dictionary<string, string> CurrentSection = null;
                if (OriginalFileExists)
                {
                    StreamReader sr = null;
                    try
                    {
                        // *** Open the original file ***
                        sr = new StreamReader(m_FileName);

                        // *** Read the file original content, replace changes with local cache values ***
                        string s;
                        string SectionName;
                        string Key = null;
                        string Value = null;
                        bool Unmodified;
                        bool Reading = true;
                        while (Reading)
                        {
                            s = sr.ReadLine();
                            Reading = (s != null);

                            // *** Check for end of file ***
                            if (Reading)
                            {
                                Unmodified = true;
                                s = s.Trim();
                                SectionName = ParseSectionName(s);
                            }
                            else
                            {
                                Unmodified = false;
                                SectionName = null;
                            }

                            // *** Check for section names ***
                            if ((SectionName != null) || (!Reading))
                            {
                                if (CurrentSection != null)
                                {
                                    // *** Write all remaining modified values before leaving a section ****
                                    if (CurrentSection.Count > 0)
                                    {
                                        foreach (string fkey in CurrentSection.Keys)
                                        {
                                            if (CurrentSection.TryGetValue(fkey, out Value))
                                            {
                                                sw.Write(fkey);
                                                sw.Write('=');
                                                sw.WriteLine(Value);
                                            }
                                        }
                                        sw.WriteLine();
                                        CurrentSection.Clear();
                                    }
                                }

                                if (Reading)
                                {
                                    // *** Check if current section is in local modified cache ***
                                    if (!m_Modified.TryGetValue(SectionName, out CurrentSection))
                                    {
                                        CurrentSection = null;
                                    }
                                }
                            }
                            else if (CurrentSection != null)
                            {
                                // *** Check for key+value pair ***
                                if (ParseKeyValuePair(s, ref Key, ref Value))
                                {
                                    if (CurrentSection.TryGetValue(Key, out Value))
                                    {
                                        // *** Write modified value to temporary file ***
                                        Unmodified = false;
                                        CurrentSection.Remove(Key);

                                        sw.Write(Key);
                                        sw.Write('=');
                                        sw.WriteLine(Value);
                                    }
                                }
                            }

                            // *** Write unmodified lines from the original file ***
                            if (Unmodified)
                            {
                                sw.WriteLine(s);
                            }
                        }

                        // *** Close the original file ***
                        sr.Close();
                        sr = null;
                    }
                    finally
                    {
                        // *** Cleanup: close files ***                  
                        if (sr != null) sr.Close();
                        sr = null;
                    }
                }

                // *** Cycle on all remaining modified values ***
                foreach (KeyValuePair<string, Dictionary<string, string>> SectionPair in m_Modified)
                {
                    CurrentSection = SectionPair.Value;
                    if (CurrentSection.Count > 0)
                    {
                        sw.WriteLine();

                        // *** Write the section name ***
                        sw.Write('[');
                        sw.Write(SectionPair.Key);
                        sw.WriteLine(']');

                        // *** Cycle on all key+value pairs in the section ***
                        foreach (KeyValuePair<string, string> ValuePair in CurrentSection)
                        {
                            // *** Write the key+value pair ***
                            sw.Write(ValuePair.Key);
                            sw.Write('=');
                            sw.WriteLine(ValuePair.Value);
                        }
                        CurrentSection.Clear();
                    }
                }
                m_Modified.Clear();

                // *** Close the temporary file ***
                sw.Close();
                sw = null;

                // *** Rename the temporary file ***
                File.Copy(TmpFileName, m_FileName, true);

                // *** Delete the temporary file ***
                File.Delete(TmpFileName);
            }
            finally
            {
                // *** Cleanup: close files ***                  
                if (sw != null) sw.Close();
                sw = null;
            }
        }

        // *** Read a value from local cache ***
        public string GetValue(string SectionName, string Key, string DefaultValue)
        {
            // *** Lazy loading ***
            if (m_Lazy)
            {
                m_Lazy = false;
                Refresh();
            }

            lock (m_Lock)
            {
                // *** Check if the section exists ***
                Dictionary<string, string> Section;
                if (!m_Sections.TryGetValue(SectionName, out Section)) return DefaultValue;

                // *** Check if the key exists ***
                string Value;
                if (!Section.TryGetValue(Key, out Value)) return DefaultValue;

                // *** Return the found value ***
                return Value;
            }
        }

        // *** Insert or modify a value in local cache ***
        public void SetValue(string SectionName, string Key, string Value)
        {
            // *** Lazy loading ***
            if (m_Lazy)
            {
                m_Lazy = false;
                Refresh();
            }

            lock (m_Lock)
            {
                // *** Flag local cache modification ***
                m_CacheModified = true;

                // *** Check if the section exists ***
                Dictionary<string, string> Section;
                if (!m_Sections.TryGetValue(SectionName, out Section))
                {
                    // *** If it doesn't, add it ***
                    Section = new Dictionary<string, string>();
                    m_Sections.Add(SectionName, Section);
                }

                // *** Modify the value ***
                if (Section.ContainsKey(Key)) Section.Remove(Key);
                Section.Add(Key, Value);

                // *** Add the modified value to local modified values cache ***
                if (!m_Modified.TryGetValue(SectionName, out Section))
                {
                    Section = new Dictionary<string, string>();
                    m_Modified.Add(SectionName, Section);
                }

                if (Section.ContainsKey(Key)) Section.Remove(Key);
                Section.Add(Key, Value);

                // *** Automatic flushing : immediately write any modification to the file ***
                if (m_AutoFlush) PerformFlush();
            }
        }

        // *** Encode byte array ***
        private string EncodeByteArray(byte[] Value)
        {
            if (Value == null) return null;

            StringBuilder sb = new StringBuilder();
            foreach (byte b in Value)
            {
                string hex = Convert.ToString(b, 16);
                int l = hex.Length;
                if (l > 2)
                {
                    sb.Append(hex.Substring(l - 2, 2));
                }
                else
                {
                    if (l < 2) sb.Append("0");
                    sb.Append(hex);
                }
            }
            return sb.ToString();
        }

        // *** Decode byte array ***
        private byte[] DecodeByteArray(string Value)
        {
            if (Value == null) return null;

            int l = Value.Length;
            if (l < 2) return new byte[] { };

            l /= 2;
            byte[] Result = new byte[l];
            for (int i = 0; i < l; i++) Result[i] = Convert.ToByte(Value.Substring(i * 2, 2), 16);
            return Result;
        }

        // *** Getters for various types ***
        public bool GetValue(string SectionName, string Key, bool DefaultValue)
        {
            string StringValue = GetValue(SectionName, Key, DefaultValue.ToString(System.Globalization.CultureInfo.InvariantCulture));
            int Value;
            if (int.TryParse(StringValue, out Value)) return (Value != 0);
            return DefaultValue;
        }

        public int GetValue(string SectionName, string Key, int DefaultValue)
        {
            string StringValue = GetValue(SectionName, Key, DefaultValue.ToString(CultureInfo.InvariantCulture));
            int Value;
            if (int.TryParse(StringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out Value)) return Value;
            return DefaultValue;
        }

        public long GetValue(string SectionName, string Key, long DefaultValue)
        {
            string StringValue = GetValue(SectionName, Key, DefaultValue.ToString(CultureInfo.InvariantCulture));
            long Value;
            if (long.TryParse(StringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out Value)) return Value;
            return DefaultValue;
        }

        public double GetValue(string SectionName, string Key, double DefaultValue)
        {
            string StringValue = GetValue(SectionName, Key, DefaultValue.ToString(CultureInfo.InvariantCulture));
            double Value;
            if (double.TryParse(StringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out Value)) return Value;
            return DefaultValue;
        }

        public byte[] GetValue(string SectionName, string Key, byte[] DefaultValue)
        {
            string StringValue = GetValue(SectionName, Key, EncodeByteArray(DefaultValue));
            try
            {
                return DecodeByteArray(StringValue);
            }
            catch (FormatException)
            {
                return DefaultValue;
            }
        }

        public DateTime GetValue(string SectionName, string Key, DateTime DefaultValue)
        {
            string StringValue = GetValue(SectionName, Key, DefaultValue.ToString(CultureInfo.InvariantCulture));
            DateTime Value;
            if (DateTime.TryParse(StringValue, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault | DateTimeStyles.AssumeLocal, out Value)) return Value;
            return DefaultValue;
        }

        // *** Setters for various types ***
        public void SetValue(string SectionName, string Key, bool Value)
        {
            SetValue(SectionName, Key, (Value) ? ("1") : ("0"));
        }

        public void SetValue(string SectionName, string Key, int Value)
        {
            SetValue(SectionName, Key, Value.ToString(CultureInfo.InvariantCulture));
        }

        public void SetValue(string SectionName, string Key, long Value)
        {
            SetValue(SectionName, Key, Value.ToString(CultureInfo.InvariantCulture));
        }

        public void SetValue(string SectionName, string Key, double Value)
        {
            SetValue(SectionName, Key, Value.ToString(CultureInfo.InvariantCulture));
        }

        public void SetValue(string SectionName, string Key, byte[] Value)
        {
            SetValue(SectionName, Key, EncodeByteArray(Value));
        }

        public void SetValue(string SectionName, string Key, DateTime Value)
        {
            SetValue(SectionName, Key, Value.ToString(CultureInfo.InvariantCulture));
        }

        #endregion

    }
}
