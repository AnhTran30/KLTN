using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PRDB_Sqlite
{
    public class Common
    {
        public static string Select = "string";
        public static string Where = "where";
        public static string From = "from";
        public static string InnerJoin = "inner join";
        public static string LeftJoin = "left join";
        public static string ReightJoin = "right join";
        public static string NaturalJoinIn = "natural join in";
        public static string NaturalJoinIg = "natural join ig";
        public static string NaturalJoinMe = "natural join me";
        public static string[] ConditionSpecialString = new string[] { "<", ">", "<=", ">=", "=", "!=", "*"};
        public static string[] ConditionNormalString = new string[] { "is", "not", "null", "like"};
        public static char[] SpecialCharacter = new char[] { '~', '!', '@', '#', '$', '%', '^', '&', '[', ']', '(', ')', '+', '`', ';', '<', '>', '?', '/', ':', '\"', '\'', '=', '{', '}', '\\', '|' };
        public static string SpecialcharacterString
        {
            get
            {
                string tmp = string.Empty;
                foreach(var item in SpecialCharacter)
                {
                    tmp += item;
                }
                return tmp;
            }
        } 
                
    }
}
