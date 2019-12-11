using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace PRDB_Sqlite.BLL
{
    public class SelectCondition
    {
        public ProbRelation relations { get; set; }
        public ProbTuple tuple { get; set; }
        public string conditionString { get; set; }
        public string MessageError { get; set; }

        static public string[] Operator = new string[15] { "_<", "_>", "<=", ">=", "_=", "!=", "⊗_in", "⊗_ig", "⊗_me", "⊕_in", "⊕_ig", "⊕_me", "equal_in", "equal_ig", "equal_me" };
        private readonly List<ProbAttribute> Attributes = new List<ProbAttribute>();
        public Dictionary<string, bool> dictProb = new Dictionary<string, bool>();
        public Dictionary<string, bool> dictCon = new Dictionary<string, bool>();
        public List<ConditionModel> conditionModels = new List<ConditionModel>();
        public string[] subCondition;

        public SelectCondition()
        {
            // TODO: Complete member initialization
        }
        public SelectCondition(ProbRelation probRelation, string conditionString)
        {
            // TODO: Complete member initialization
            relations = probRelation;

            int i = 0;
            while (i < conditionString.Length - 1)
            {
                if (conditionString[i] == '<' && conditionString[i + 1] != '=')
                    conditionString = conditionString.Insert(i++, "_");
                if (conditionString[i] == '>' && conditionString[i + 1] != '=')
                    conditionString = conditionString.Insert(i++, "_");
                if (conditionString[i] == '=' && conditionString[i - 1] != '!' && conditionString[i - 1] != '<' && conditionString[i - 1] != '>')
                    conditionString = conditionString.Insert(i++, "_");
                i++;
            }
            this.conditionString = conditionString.Trim();
            this.Attributes = probRelation.Scheme.Attributes;
            this.MessageError = string.Empty;
        }

        #region kiểm tra bộ có thỏa mãn điều kiện chọn
        public bool Satisfied(ProbTuple tuple)
        {
            this.tuple = tuple;
            dictProb = new Dictionary<string, bool>();
            dictCon = new Dictionary<string, bool>();

            CalculateConditionProb();

            CalculateConditionStr(subCondition);
            var listConditionProb = GetArrayCondition(this.conditionString);
            if (listConditionProb == null)
            {
                return false;
            }

            return CalculateTotalCondition(listConditionProb);
        }

        public void ProcessConditionString()
        {
            string conditionStr = this.conditionString;
            string[] subConditionHaveProbability;

            Regex regexPro = new Regex(@"\([^\(\)]+\)\[[^\(\)]+\]");

            var conditionProbabilities = regexPro.Matches(conditionStr);
            subConditionHaveProbability = new string[conditionProbabilities.Count];
            //create subcondition array
            for (int i = 0; i < conditionProbabilities.Count; i++)
            {
                subConditionHaveProbability[i] = conditionProbabilities[i].ToString();
                conditionStr = conditionStr.Replace(conditionProbabilities[i].ToString(), "ConditionProb_" + i.ToString());
            }

            var count1 = 0;
            var count2 = 0;
            var degree = 0;

            // dem so lan va so bac cua ky tu '(' 
            for (int i = 0; i < conditionStr.Length; i++)
            {
                if (conditionStr[i] == '(')
                {
                    count1++;
                    var tmp = 1;
                    for (int j = i + 1; j < conditionStr.Length; j++)
                    {
                        if (conditionStr[j] == '(')
                            tmp++;
                        if (conditionStr[j] == ')') break;
                    }
                    degree = degree >= tmp ? degree : tmp;
                }
                if (conditionStr[i] == ')') count2++;
            }

            if (count1 != count2)
            {
                MessageError = string.Format("Incorrect syntax near the keyword.");
                return;
            }

            Regex regexCondition = new Regex(@"\([^\(\)]+\)");


            var totalLength = GetTotalSubCondition(conditionStr, degree, regexCondition);
            subCondition = new string[totalLength];

            var timeCondition = 0;
            //creatre group condition array 
            for (int i = 0; i < degree; i++)
            {
                var listCondition = regexCondition.Matches(conditionStr);

                for (int j = 0; j < listCondition.Count; j++)
                {
                    var tmp = listCondition[j].ToString();
                    tmp = tmp.Remove(0, 1);
                    tmp = tmp.Remove(tmp.Length - 1, 1);
                    subCondition[j] = tmp;

                    conditionStr = conditionStr.Replace(listCondition[i].ToString(), "Condition_" + timeCondition.ToString());
                    timeCondition++;
                }
            }

            this.conditionString = conditionStr;

            if (subConditionHaveProbability.Count() > 0)
            {
                ConvertStringToModel(subConditionHaveProbability);
            }
        }

        private static int GetTotalSubCondition(string conditionStr, int degree, Regex regexCondition)
        {
            var timeCondition = 1;
            for (int i = 0; i < degree; i++)
            {
                var listCondition = regexCondition.Matches(conditionStr);

                for (int j = 0; j < listCondition.Count; j++)
                {
                    timeCondition++;
                }
            }

            return timeCondition;
        }

        private string[] GetArrayCondition(string conditionStr)
        {
            string[] listConditionProb;
            var option = conditionStr.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (option.Count() == 1 && !option[0].Contains("ConditionProb_") && !option[0].Contains("Condition_"))
            {
                MessageError = string.Format("An expression of non-boolean type specified in a context where a condition is expected, near '{0}'.", option[0]);
                return null;
            }
            var count = 0;
            for (int i = 0; i < option.Count(); i++)
            {
                if (Common.ConditionNormalString.Contains(option[i]))
                {
                    count++;
                }
            }

            var count3 = 0;
            listConditionProb = new string[(count * 2) + 1];
            foreach (var item in option)
            {
                if (Common.ConditionNormalString.Contains(item))
                {
                    count3++;
                    listConditionProb[count3] = item;
                    count3++;
                    continue;
                }
                listConditionProb[count3] += item;
            }

            foreach (string item in listConditionProb)
            {
                if (Common.ConditionNormalString.Contains(item)) continue;

                if(item.Contains("ConditionProb_") && item.Trim().Length > 15)
                {
                    MessageError = string.Format("An expression of non-boolean type specified in a context where a condition is expected, near 'Where'.");
                    return null;
                }

                if (item.Contains("Condition_") && item.Trim().Length > 11)
                {
                    MessageError = string.Format("An expression of non-boolean type specified in a context where a condition is expected, near 'Where'.");
                    return null;
                }

                if (!item.Contains("Condition_") && !item.Contains("ConditionProb_") && string.IsNullOrEmpty(IsOperatior(item)))
                {
                    MessageError = string.Format("An expression of non-boolean type specified in a context where a condition is expected, near 'Where'.");
                    return null;
                }
            }

            return listConditionProb.Where(x => !string.IsNullOrEmpty(x)).ToArray();
        }

        private void CalculateConditionProb()
        {
            for (int i = 0; i < conditionModels.Count(); i++)
            {
                if (conditionModels[i].OperatorStrategy != null)
                {
                    string[] conditionArray = new string[conditionModels[i].OperatorStrategy.Count * 2 + 1];

                    var listProb = new List<double>();
                    int k = 0;
                    for (int j = 0; j < conditionModels[i].StrategyModels.Count; j++)
                    {
                        var strategyModel = conditionModels[i].StrategyModels[j];
                        var prob = GetProbIntervalV3(strategyModel.AttributeName, strategyModel.AttributeValue, strategyModel.OperatorStrOfTriple);

                        if (prob == null)
                        {
                            return;
                        }

                        conditionArray[k] = prob[0].ToString() + "-" + prob[1].ToString();
                        if (j != (conditionModels[i].StrategyModels.Count - 1))
                            conditionArray[k + 1] = conditionModels[i].OperatorStrategy[j];
                        k = k + 2;
                    }

                    List<string[]> listResult = new List<string[]>();
                    int d = 0;
                    for (int j = 0; j < conditionArray.Length; j++)
                    {
                        if (conditionArray[j].Contains("⊕"))
                        {
                            listResult.Add(conditionArray.Skip(j - d).Take(d).ToArray());
                            listResult.Add(conditionArray.Skip(j).Take(1).ToArray());
                            d = 0;
                        }
                        else
                        {
                            if (j == conditionArray.Length - 1)
                            {
                                listResult.Add(conditionArray.Skip(j - d).Take(d + 1).ToArray());
                                break;
                            }
                            d++;
                        }
                    }

                    var listString = new List<string>();
                    foreach (var item in listResult)
                    {

                        if (item.Length > 1)
                        {
                            listString.Add(ConjunctionStrategy(item));
                        }
                        else
                        {
                            listString.Add(item[0]);
                        }
                    }

                    dictProb.Add("ConditionProb_" + i.ToString(), DisjunctionStrategy(listString, conditionModels[i].MinProb.Value, conditionModels[i].MaxProb.Value));
                }
                else
                {
                    dictProb.Add("ConditionProb_" + i.ToString(), GetProbIntervalV2(conditionModels[i].StrategyModels[0].AttributeName, conditionModels[i].StrategyModels[0].AttributeValue, conditionModels[i].StrategyModels[0].OperatorStrOfTriple, conditionModels[i].MaxProb, conditionModels[i].MinProb));
                }
            }
        }

        public string ConjunctionStrategy(string[] arrayInput)
        {
            var tmpOne = arrayInput[0].Split(new char[] { '-' }, StringSplitOptions.RemoveEmptyEntries);
            double maxProb = double.Parse(tmpOne[1]);
            double minProb = double.Parse(tmpOne[0]);
            double minProbOne = 0; double maxProbOne = 0;

            for (int i = 1; i < arrayInput.Length; i++)
            {
                if (arrayInput[i].Contains("⊗"))
                {
                    var tmp = arrayInput[i + 1].Split(new char[] { '-' }, StringSplitOptions.RemoveEmptyEntries);
                    minProbOne = double.Parse(tmp[0]);
                    maxProbOne = double.Parse(tmp[1]);

                    if (arrayInput[i].Contains("⊗_ig"))
                    {
                        minProb = Math.Max(0, minProbOne + minProb - 1); maxProb = Math.Min(maxProbOne, maxProb);
                    }
                    else if (arrayInput[i].Contains("⊗_in"))
                    {
                        minProb = minProbOne * minProb; maxProb = maxProbOne * maxProb;
                    }
                    else
                    {
                        minProb = 0; maxProb = 0;
                    }

                    i = i + 1;
                }
            }

            return minProb.ToString() + "-" + maxProb.ToString();
        }

        public bool DisjunctionStrategy(List<string> listDisjunction, double minProd, double maxProb)
        {
            var tmpOne = listDisjunction[0].Split(new char[] { '-' }, StringSplitOptions.RemoveEmptyEntries);
            double minProbOne = double.Parse(tmpOne[1]);
            double maxProbOne = double.Parse(tmpOne[0]);
            double minProbTwo = 0; double maxProbTwo = 0;

            for (int i = 1; i < listDisjunction.Count; i++)
            {
                if (listDisjunction[i].Contains("⊕"))
                {
                    var tmp = listDisjunction[i + 1].Split(new char[] { '-' }, StringSplitOptions.RemoveEmptyEntries);
                    minProbTwo = double.Parse(tmp[0]);
                    maxProbTwo = double.Parse(tmp[1]);

                    if (listDisjunction[i].Contains("⊕_ig"))
                    {
                        minProbOne = Math.Max(minProbOne, minProbTwo); maxProbOne = Math.Min(1, maxProbOne + maxProbTwo);
                    }
                    else if (listDisjunction[i].Contains("⊕_in"))
                    {
                        minProbOne = minProbOne + minProbTwo - (minProbOne * minProbTwo); maxProbOne = maxProbOne + maxProbTwo - (maxProbOne * maxProbTwo);
                    }
                    else
                    {
                        minProbOne = Math.Min(1, minProbOne + minProbTwo); maxProbOne = Math.Min(1, maxProbOne + maxProbTwo);
                    }

                    i = i + 1;
                }
            }

            return minProd <= minProbOne && maxProbOne <= maxProb;
        }

        public void ConvertStringToModel(string[] subConditionHaveProbability)
        {
            subConditionHaveProbability = subConditionHaveProbability.Where(x => !string.IsNullOrEmpty(x)).ToArray();
            for (int i = 0; i < subConditionHaveProbability.Count(); i++)
            {
                try
                {
                    if (!subConditionHaveProbability[i].Contains("(") && !subConditionHaveProbability[i].Contains("(") && !subConditionHaveProbability[i].Contains("[") && !subConditionHaveProbability[i].Contains("]"))
                    {
                        MessageError = "Incorrect syntax near 'where'.";
                        return;
                    }

                    var j1 = subConditionHaveProbability[i].IndexOf('(');
                    var j2 = subConditionHaveProbability[i].IndexOf(')');
                    var j3 = subConditionHaveProbability[i].IndexOf('[');
                    var j4 = subConditionHaveProbability[i].LastIndexOf(',');
                    var j5 = subConditionHaveProbability[i].IndexOf(']');

                    var minProb = double.Parse(subConditionHaveProbability[i].Substring(j3 + 1, j4 - j3 - 1));
                    var maxProb = double.Parse(subConditionHaveProbability[i].Substring(j4 + 1, j5 - j4 - 1));
                    var valueString = subConditionHaveProbability[i].Substring(j1 + 1, j2 - j1 - 1);


                    var listStrategyModel = new List<StrategyModel>();
                    if (!string.IsNullOrEmpty(IsOperatiorStrategy(valueString)))
                    {
                        var listOperatorStrategy = new List<string>();

                        var listStrategy = valueString.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                        int totalStrategy = 0;

                        foreach (var item in listStrategy)
                        {
                            if (!string.IsNullOrEmpty(IsOperatiorStrategy(item)))
                            {
                                totalStrategy++;
                            }
                        }

                        string[] listConditionStrategy = new string[(totalStrategy * 2) + 1];
                        var tmp = 0;
                        for (int k = 0; k < listStrategy.Length; k++)
                        {
                            if (!string.IsNullOrEmpty(IsOperatiorStrategy(listStrategy[k])))
                            {
                                tmp++;
                                listConditionStrategy[tmp] = listStrategy[k];
                                tmp++;
                            }
                            else
                            {
                                listConditionStrategy[tmp] += listStrategy[k];
                            }
                        }

                        for (int l = 0; l < listConditionStrategy.Length; l++)
                        {
                            var checkStrategy = IsOperatiorStrategy(listConditionStrategy[l]);
                            if (string.IsNullOrEmpty(checkStrategy))
                            {
                                var operatorStrTwo = IsOperatior(listConditionStrategy[l]);
                                if (!string.IsNullOrEmpty(operatorStrTwo))
                                {
                                    string[] seperatorTwo = { operatorStrTwo };
                                    string[] attributeTwo = listConditionStrategy[l].Split(seperatorTwo, StringSplitOptions.RemoveEmptyEntries);
                                    if (attributeTwo.Length != 2)
                                    {
                                        MessageError = string.Format("Incorrect syntax near the keyword {0}.", listConditionStrategy[l]);
                                        return;
                                    }

                                    listStrategyModel.Add(new StrategyModel { AttributeName = attributeTwo[0], AttributeValue = attributeTwo[1], OperatorStrOfTriple = operatorStrTwo });
                                }
                                else
                                {
                                    MessageError = string.Format("Incorrect syntax near the keyword {0}.", listConditionStrategy[l]);
                                    return;
                                }
                            }
                            else
                            {
                                listOperatorStrategy.Add(listConditionStrategy[l]);
                            }
                        }

                        var model = new ConditionModel
                        {
                            StrategyModels = listStrategyModel,
                            OperatorStrategy = listOperatorStrategy,
                            MaxProb = maxProb,
                            MinProb = minProb
                        };

                        conditionModels.Add(model);
                    }
                    else
                    {
                        var operatorStrOfTriple = IsOperatior(valueString);
                        if (string.IsNullOrEmpty(operatorStrOfTriple))
                        {
                            MessageError = string.Format("Incorrect syntax near the keyword {0}.", valueString);
                            return;
                        }
                        string[] seperator = { operatorStrOfTriple };
                        string[] attribute = valueString.Split(seperator, StringSplitOptions.RemoveEmptyEntries);

                        if (attribute.Count() != 2)
                        {
                            MessageError = string.Format("Incorrect syntax near the keyword {0}.", operatorStrOfTriple);
                            return;
                        }
                        var attributeName = attribute[0];
                        var attributeValue = attribute[1];

                        listStrategyModel.Add(new StrategyModel
                        {
                            AttributeName = attributeName.Trim(),
                            AttributeValue = attributeValue.Trim(),
                            OperatorStrOfTriple = operatorStrOfTriple.Trim()
                        });

                        var model = new ConditionModel
                        {
                            StrategyModels = listStrategyModel,
                            MaxProb = maxProb,
                            MinProb = minProb
                        };

                        conditionModels.Add(model);
                    }
                }
                catch
                {
                    MessageError = "Incorrect syntax near 'where'.";
                    return;
                }
            }
        }

        private void CalculateConditionStr(string[] subCondition)
        {
            Dictionary<string, bool> dict = new Dictionary<string, bool>();

            subCondition = subCondition.Where(x => !string.IsNullOrEmpty(x)).ToArray();

            if (subCondition.Length == 0)
            {
                return;
            }

            try
            {
                for (int i = 0; i < subCondition.Count(); i++)
                {
                    var listConditionProb = GetArrayCondition(subCondition[i]);

                    string result = string.Empty;
                    for (int j = 0; j < listConditionProb.Count(); j++)
                    {
                        if (Common.ConditionNormalString.Contains(listConditionProb[j]))
                        {
                            var s = CompareCharacters(listConditionProb[j]);
                            if (string.IsNullOrEmpty(s))
                            {
                                MessageError = string.Format("Incorrect syntax near the keyword.");
                                return;
                            }
                            result += s;
                        }
                        else
                        {
                            if (listConditionProb[j].Contains("ConditionProb_"))
                            {
                                foreach (var item in dictProb)
                                {
                                    if (item.Key == listConditionProb[j].Trim())
                                    {
                                        result += item.Value ? "1" : "0";
                                    }
                                }

                            }
                            else
                            {
                                if (listConditionProb[j].Contains("Condition_"))
                                {
                                    foreach (var item in dictCon)
                                    {
                                        if (item.Key == listConditionProb[j].Trim())
                                        {
                                            result += item.Value ? "1" : "0";
                                        }
                                    }
                                }
                                else
                                {
                                    var operatorStr = IsOperatior(listConditionProb[j]);
                                    if (!string.IsNullOrEmpty(operatorStr))
                                    {
                                        string[] seperator = { operatorStr };
                                        string[] attribute = listConditionProb[j].Split(seperator, StringSplitOptions.RemoveEmptyEntries);

                                        if (attribute.Count() != 2)
                                        {
                                            MessageError = string.Format("Incorrect syntax near the keyword {0}.", operatorStr);
                                            return;
                                        }
                                        var attributeName = attribute[0];
                                        var attributeValue = attribute[1];
                                        result += GetProbIntervalV2(attributeName.Trim(), attributeValue.Trim(), operatorStr) ? "1" : "0";
                                    }
                                }
                            }
                        }
                    }
                    dict.Add("Condition_" + i.ToString(), CalculationCon(result));
                }
                dictCon = dict;
            }
            catch
            {
                MessageError = "Incorrect syntax near 'where'.";
                return;
            }

        }

        private bool CalculateTotalCondition(string[] listConditionProb)
        {
            var totalOfList = listConditionProb.Count();
            string result = string.Empty;

            for (int i = 0; i < totalOfList; i++)
            {
                if (Common.ConditionNormalString.Contains(listConditionProb[i]))
                {
                    var s = CompareCharacters(listConditionProb[i]);
                    if (string.IsNullOrEmpty(s))
                    {
                        MessageError = string.Format("Incorrect syntax near the keyword.");
                        return false;
                    }
                    result += s;
                }
                else
                {
                    var operatorStr = IsOperatior(listConditionProb[i]);
                    if (!string.IsNullOrEmpty(operatorStr))
                    {
                        string[] seperator = { operatorStr };
                        string[] attribute = listConditionProb[i].Split(seperator, StringSplitOptions.RemoveEmptyEntries);

                        if (attribute.Count() != 2)
                        {
                            MessageError = string.Format("Incorrect syntax near the keyword {0}.", operatorStr);
                            return false;
                        }
                        var attributeName = attribute[0];
                        var attributeValue = attribute[1];
                        result += GetProbIntervalV2(attributeName.Trim(), attributeValue.Trim(), operatorStr) ? "1" : "0";
                    }
                    else
                    {
                        if (listConditionProb[i].Contains("ConditionProb_"))
                        {
                            foreach (var item in dictProb)
                            {
                                if (item.Key == listConditionProb[i].Trim())
                                {
                                    result += item.Value ? "1" : "0";
                                }
                            }
                        }
                        if (listConditionProb[i].Contains("Condition_"))
                        {
                            foreach (var item in dictCon)
                            {
                                if (item.Key == listConditionProb[i].Trim())
                                {
                                    result += item.Value ? "1" : "0";
                                }
                            }
                        }
                    }
                }
            }

            return CalculationCon(result);
        }

        private string IsOperatior(string s)
        {
            for (int i = 0; i < 6; i++)
            {
                if (s.Contains(Operator[i]))
                    return Operator[i];
            }
            return string.Empty;
        }

        private string IsOperatiorStrategy(string s)
        {
            for (int i = 6; i < 12; i++)
            {
                if (s.Contains(Operator[i]))
                    return Operator[i];
            }
            return string.Empty;
        }

        private string CompareCharacters(string s)
        {
            switch (s)
            {
                case "or": return "|";
                case "and": return "&";
                case "not": return "!";
                //bo xung them
                default: return string.Empty;
            }
        }

        private bool GetProbIntervalV2(string valueOne, string valueTwo, string operaterStr, double? maxProbOfCon = null, double? minProbOfCon = null)
        {
            double minProb = 0, maxProb = 0;
            int indexOne, countTripleOne;
            ProbTuple tuple = this.tuple;

            try
            {
                if (SelectCondition.isCompareOperator(operaterStr))     // Biểu thức so sánh giữa một thuộc tính với một giá trị
                {
                    indexOne = this.IndexOfAttribute(valueOne); // vị trí của thuộc tính trong ds các thuộc tính
                    if (indexOne == -1)
                        return false;

                    if (valueTwo.Contains("'"))
                    {

                        int count = valueTwo.Split(new char[] { '\'' }).Length - 1;


                        if (valueTwo.Substring(0, 1) != "'")
                        {
                            MessageError = "Unclosed quotation mark before the character string " + valueTwo;
                            return false;
                        }

                        if (valueTwo.Substring(valueTwo.Length - 1, 1) != "'")
                        {
                            MessageError = "Unclosed quotation mark after the character string " + valueTwo;
                            return false;
                        }


                        if (count != 2)
                        {
                            MessageError = "Unclosed quotation mark at the character string " + valueTwo;
                            return false;
                        }

                        valueTwo = valueTwo.Remove(0, 1);
                        valueTwo = valueTwo.Remove(valueTwo.Length - 1, 1);
                    }

                    #region ProbDataType
                    ProbDataType dataType = new ProbDataType();
                    dataType.TypeName = Attributes[indexOne].Type.TypeName;
                    dataType.DataType = Attributes[indexOne].Type.DataType;
                    dataType.Domain = Attributes[indexOne].Type.Domain;
                    dataType.DomainString = Attributes[indexOne].Type.DomainString;
                    #endregion

                    if (!dataType.CheckDataTypeOfVariables(valueTwo))
                    {
                        MessageError = String.Format("Conversion failed when converting the varchar value {0} to data type {1}.", valueTwo, dataType.DataType);
                        return false;
                    }

                    #region ProbDataType
                    countTripleOne = tuple.Triples[indexOne].Value.Count; // số lượng các cặp xác xuất trong thuộc tính
                    var listValue = tuple.Triples[indexOne].Value;
                    var minProbV = tuple.Triples[indexOne].MinProb;
                    var maxProbV = tuple.Triples[indexOne].MaxProb;
                    #endregion

                    if (maxProbV == 1 && minProbV == 1 && !maxProbOfCon.HasValue && !minProbOfCon.HasValue)
                    {
                        var kp = listValue.Any(x => CompareTriple(x, valueTwo, operaterStr, dataType.TypeName));
                        return kp;
                    }
                    else
                    {
                        var count = listValue.Where(x => CompareTriple(x, valueTwo, operaterStr, dataType.TypeName)).Count();
                        if (count > 0)
                        {
                            minProb = (count / (float)countTripleOne) * minProbV;
                            maxProb = (count / (float)countTripleOne) * maxProbV;

                            if (maxProbOfCon.HasValue && minProbOfCon.HasValue)
                            {
                                return minProbOfCon.Value <= minProb && maxProb <= maxProbOfCon;
                            }
                        }
                        return false;
                    }
                }
                else                     // Biểu thức kết hợp giữa hai khoảng xác suất
                {
                    double minProbOne, minProbTwo, maxProbOne, maxProbTwo;
                    string[] StrProb;

                    valueOne = valueOne.Replace("[", "");  // [L,U]
                    valueOne = valueOne.Replace("]", "");

                    StrProb = valueOne.Split(',');
                    minProbOne = Convert.ToDouble(StrProb[0]);
                    maxProbOne = Convert.ToDouble(StrProb[1]);

                    valueTwo = valueTwo.Replace("[", "");  // [L,U]
                    valueTwo = valueTwo.Replace("]", "");

                    StrProb = valueTwo.Split(',');
                    minProbTwo = Convert.ToDouble(StrProb[0]);
                    maxProbTwo = Convert.ToDouble(StrProb[1]);

                    switch (operaterStr)
                    {
                        case "⊗_ig": minProb = Math.Max(0, minProbOne + minProbTwo - 1); maxProb = Math.Min(maxProbOne, maxProbTwo); break;
                        case "⊗_in": minProb = minProbOne * minProbTwo; maxProb = maxProbOne * maxProbTwo; break;
                        case "⊗_me": minProb = 0; maxProb = 0; break;
                        case "⊕_ig": minProb = Math.Max(minProbOne, minProbTwo); maxProb = Math.Min(1, maxProbOne + maxProbTwo); break;
                        case "⊕_in": minProb = minProbOne + minProbTwo - (minProbOne * minProbTwo); maxProb = maxProbOne + maxProbTwo - (maxProbOne * maxProbTwo); break;
                        case "⊕_me": minProb = Math.Min(1, minProbOne + minProbTwo); maxProb = Math.Min(1, maxProbOne + maxProbTwo); break;
                        default:
                            MessageError = "Incorrect syntax near 'where'.";
                            break;

                    }
                }
            }
            catch
            {
                MessageError = "Incorrect syntax near 'where'.";
                return false;
            }
            return false;
        }

        private List<double> GetProbIntervalV3(string valueOne, string valueTwo, string operaterStr)
        {
            double minProb = 0, maxProb = 0;
            int indexOne, countTripleOne;
            ProbTuple tuple = this.tuple;

            try
            {
                if (SelectCondition.isCompareOperator(operaterStr))     // Biểu thức so sánh giữa một thuộc tính với một giá trị
                {
                    indexOne = this.IndexOfAttribute(valueOne); // vị trí của thuộc tính trong ds các thuộc tính
                    if (indexOne == -1)
                        return null;

                    if (valueTwo.Contains("'"))
                    {
                        int count = valueTwo.Split(new char[] { '\'' }).Length - 1;

                        if (valueTwo.Substring(0, 1) != "'")
                        {
                            MessageError = "Unclosed quotation mark before the character string " + valueTwo;
                            return null;
                        }

                        if (valueTwo.Substring(valueTwo.Length - 1, 1) != "'")
                        {
                            MessageError = "Unclosed quotation mark after the character string " + valueTwo;
                            return null;
                        }


                        if (count != 2)
                        {
                            MessageError = "Unclosed quotation mark at the character string " + valueTwo;
                            return null;
                        }

                        valueTwo = valueTwo.Remove(0, 1);
                        valueTwo = valueTwo.Remove(valueTwo.Length - 1, 1);
                    }

                    #region ProbDataType
                    ProbDataType dataType = new ProbDataType();
                    dataType.TypeName = Attributes[indexOne].Type.TypeName;
                    dataType.DataType = Attributes[indexOne].Type.DataType;
                    dataType.Domain = Attributes[indexOne].Type.Domain;
                    dataType.DomainString = Attributes[indexOne].Type.DomainString;
                    #endregion

                    if (!dataType.CheckDataTypeOfVariables(valueTwo))
                    {
                        MessageError = String.Format("Conversion failed when converting the varchar value {0} to data type {1}.", valueTwo, dataType.DataType);
                        return null;
                    }

                    #region ProbDataType
                    countTripleOne = tuple.Triples[indexOne].Value.Count; // số lượng các cặp xác xuất trong thuộc tính
                    var listValue = tuple.Triples[indexOne].Value;
                    var minProbV = tuple.Triples[indexOne].MinProb;
                    var maxProbV = tuple.Triples[indexOne].MaxProb;
                    #endregion

                    var result = listValue.Where(x => CompareTriple(x, valueTwo, operaterStr, dataType.TypeName)).Count();

                    if (result > 0)
                    {
                        minProb = (result / (float)countTripleOne) * minProbV;
                        maxProb = (result / (float)countTripleOne) * maxProbV;

                        return new List<double> { minProb, maxProb };
                    }
                    else
                    {
                        return new List<double> { 0, 0 };
                    }
                }

                return null;
            }
            catch
            {
                MessageError = "Incorrect syntax near 'where'.";
                return null;
            }
        }

        public int IndexOfAttribute(string S)
        {
            string value = S.Trim().ToLower();
            int indexAttributeS = -1;
            if (value.IndexOf(".") != -1)
            {
                string[] arr = value.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
                int count = 0;
                foreach (string item in this.relations.ListRenameRelation)
                {
                    if (item != arr[0])
                        count++;
                }
                if (count == 2)
                {
                    MessageError = String.Format("The multi-part identifier {0} could not be bound.", value);
                    return -1;
                }

                for (int i = 0; i < Attributes.Count; i++)
                {
                    if (value == Attributes[i].AttributeName.ToLower())
                    {
                        return i;
                    }
                }

                MessageError = String.Format("Invalid attribute name '{0}'.", arr[1]);
                return -1;

            }
            else
            {
                int count = 0;
                for (int i = 0; i < Attributes.Count; i++)
                {
                    string[] arr = Attributes[i].AttributeName.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
                    if (value == arr[1].ToLower().Trim())
                    {
                        count++;
                        indexAttributeS = i;
                    }
                }

                if (count >= 2)
                {
                    MessageError = String.Format("Ambiguous attribute name '{0}'.", value);
                    return -1;
                }
                if (count == 0)
                {
                    MessageError = String.Format("Invalid attribute name '{0}'.", value);
                    return -1;
                }

                return indexAttributeS;
            }

        }

        private static bool CalculationCon(string s1)
        {
            string valueOne, valueTwo;
            string[] listOperation = s1.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
            List<bool> result = new List<bool>();

            foreach (var item in listOperation)
            {
                List<string> stack = new List<string>();
                for (int i = 0; i < item.Length; i++)
                {
                    if (item[i].ToString().CompareTo("!") == 0)
                    {
                        if (item[i + 1].ToString().CompareTo("1") == 0 || item[i + 1].ToString().CompareTo("0") == 0)
                        {
                            valueOne = item[i + 1].ToString().CompareTo("1") == 0 ? "0" : "1";
                            stack.Add(valueOne);
                            i = i + 1;
                        }
                        else
                        {
                            return false;
                        }
                    }
                    else if (item[i].ToString().CompareTo("&") == 0)
                    {
                        valueOne = stack[stack.Count - 1].ToString();
                        stack.RemoveAt(stack.Count - 1);
                        int d = 0;
                        if (item[i + 1].ToString().CompareTo("!") == 0 && (item[i + 2].ToString().CompareTo("1") == 0 || item[i + 2].ToString().CompareTo("0") == 0))
                        {
                            valueTwo = item[i + 2].ToString().CompareTo("1") == 0 ? "0" : "1";
                            d = 2;
                        }
                        else
                        {
                            valueTwo = item[i + 1].ToString();
                            d = 1;
                        }
                        bool v1 = (valueOne.CompareTo("1") == 0) ? true : false;
                        bool v2 = (valueTwo.CompareTo("1") == 0) ? true : false;
                        switch (item[i].ToString())
                        {
                            case "&": stack.Add(v1 && v2 ? "1" : "0"); break;
                            case "|": stack.Add(v1 || v2 ? "1" : "0"); break;
                            default: break;
                        }
                        i = i + d;
                    }
                    else
                    {
                        stack.Add(item[i].ToString());
                    }
                }

                result.Add(stack[0].CompareTo("1") == 0);
            }

            return (result.Any(x => x));
        }
        #endregion

        #region những hàm compare
        public static bool isCompareOperator(string S)
        {
            for (int i = 0; i < 6; i++)
                if (Operator[i].CompareTo(S) == 0)
                    return true;
            return false;
        }
        public static bool EQUAL(object a, object b, string type)
        {
            switch (type)
            {
                case "Int16":
                case "Int64":
                case "Int32":
                case "Byte":
                case "Currency": return int.Parse(a.ToString()) == int.Parse(b.ToString());
                case "String":
                case "DateTime":
                case "UserDefined":
                case "Binary": return (a.ToString().CompareTo(b.ToString()) == 0);
                case "Decimal":
                case "Single":
                case "Double": return (Math.Abs((double)a - (double)b) < 0.001);
                case "Boolean": return Boolean.Parse(a.ToString()) == Boolean.Parse(b.ToString());
                default: return false;
            }
        }
        public static bool IntCompare(Int16 valueOne, Int16 valueTwo, string OpratorStr)
        {
            switch (OpratorStr)
            {
                case "_<": return (valueOne < valueTwo);
                case "_>": return (valueOne > valueTwo);
                case "<=": return (valueOne <= valueTwo);
                case ">=": return (valueOne >= valueTwo);
                case "_=": return (valueOne == valueTwo);
                case "!=": return (valueOne != valueTwo);
                default: return false;
            }
        }
        public static bool StrCompare(string valueOne, string valueTwo, string OpratorStr)
        {

            switch (OpratorStr)
            {
                case "_=": return (valueOne.ToLower().CompareTo(valueTwo) == 0);
                case "!=": return (valueOne.ToLower().CompareTo(valueTwo) != 0);
                default: return false;
            }

        }
        public static bool DoubleCompare(double valueOne, double valueTwo, string OpratorStr)
        {
            switch (OpratorStr)
            {
                case "_<": return (valueOne < valueTwo);
                case "_>": return (valueOne > valueTwo);
                case "<=": return (valueOne <= valueTwo);
                case ">=": return (valueOne >= valueTwo);
                case "_=": return (Math.Abs(valueOne - valueTwo) < 0.001);
                case "!=": return (Math.Abs(valueOne - valueTwo) > 0.001);
                default: return false;
            }

        }
        public static bool BoolCompare(bool valueOne, bool valueTwo, string OpratorStr)
        {
            switch (OpratorStr)
            {
                case "_=": return (valueOne == valueTwo);
                case "!=": return (valueOne != valueTwo);
                default: return false;
            }

        }
        public bool CompareTriple(object valueOne, string valueTwo, string opratorStr, string typename)
        {
            switch (typename)
            {
                case "Int16":
                case "Int64":
                case "Int32":
                case "Byte":
                case "Currency":
                    return IntCompare(Convert.ToInt16(valueOne), Convert.ToInt16(valueTwo), opratorStr);
                case "String":
                case "DateTime":
                case "UserDefined":
                case "Binary":
                    return StrCompare(valueOne.ToString(), valueTwo, opratorStr);
                case "Decimal":
                case "Single":
                case "Double":
                    return DoubleCompare((double)valueOne, Convert.ToDouble(valueTwo), opratorStr);
                case "Boolean":
                    return BoolCompare((bool)valueOne, Convert.ToBoolean(valueTwo), opratorStr);
                default: return false;
            }

        }
        #endregion

    }
}
