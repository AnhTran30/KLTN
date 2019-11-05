using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PRDB_Sqlite.BLL
{
    public class ConditionModel
    {
        public string AttributeName { get; set; }
        public string AttributeValue { get; set; }
        public string OperatorStrOfTriple { get; set; }
        public double? MaxProb { get; set; }
        public double? MinProb { get; set; }
    }
}
