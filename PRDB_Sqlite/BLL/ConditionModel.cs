using System.Collections.Generic;

namespace PRDB_Sqlite.BLL
{
    public class ConditionModel
    {
        public List<StrategyModel> StrategyModels { get; set; }
        public List<string> OperatorStrategy { get; set; }
        public double? MaxProb { get; set; }
        public double? MinProb { get; set; }
    }

    public class StrategyModel
    {
        public string AttributeName { get; set; }
        public string AttributeValue { get; set; }
        public string OperatorStrOfTriple { get; set; }
    }
}
