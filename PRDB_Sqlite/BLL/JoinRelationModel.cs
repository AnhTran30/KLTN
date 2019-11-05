using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PRDB_Sqlite.BLL
{
    public class JoinRelationModel
    {
        public string Operation { get; set; }
        public string RelationName { get; set; }
        public string ConditionKey { get; set; }
        public string AcronymRelationName { get; set; }
        public int Stage { get; set; }

    }
}
