namespace PRDB_Sqlite
{
    public class Common
    {
        public static string Select = "select";
        public static string Where = "where";
        public static string From = "from";
        public static string InnerJoin = " inner join ";
        public static string LeftJoin = " left join ";
        public static string ReightJoin = " right join ";
        public static string FullJoin = " full outer join ";
        public static string NaturalJoinIn = " natural join in ";
        public static string NaturalJoinIg = " natural join ig ";
        public static string NaturalJoinMe = " natural join me ";
        public static string[] ConditionSpecialString = new string[] { "<", ">", "<=", ">=", "=", "!=", "*" };
        public static string[] ConditionNormalString = new string[] { "or", "and", "in", "is", "not", "null", "like", "some", "exists", "between", "any", "all" };
        public static char[] SpecialCharacter = new char[] { '~', '!', '@', '#', '$', '%', '^', '&', '[', ']', '(', ')', '+', '`', ';', '<', '>', '?', '/', ':', '\"', '\'', '=', '{', '}', '\\', '|' };
        public static string SpecialcharacterString
        {
            get
            {
                string tmp = string.Empty;
                foreach (var item in SpecialCharacter)
                {
                    tmp += item;
                }
                return tmp;
            }
        }

    }
}
