using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace PRDB_Sqlite.BLL
{
    public class QueryExecution
    {
        public string queryString { get; set; }
        public List<ProbRelation> selectedRelations { get; set; }
        public ProbRelation relationResult { get; set; }
        public ProbDatabase probDatabase { get; set; }
        public string conditionString { get; set; }
        public bool flagNaturalJoin { get; set; }
        public string MessageError { get; set; }
        public ProbRelation DescartesAndNaturalJoin { get; set; }

        public List<ProbAttribute> selectedAttributes;
        public List<JoinRelationModel> joinRelationModel = new List<JoinRelationModel>();
        private string OperationNaturalJoin = string.Empty;

        public QueryExecution(string queryString, ProbDatabase probDatabase)
        {
            this.selectedRelations = new List<ProbRelation>();
            this.selectedAttributes = new List<ProbAttribute>();
            this.relationResult = new ProbRelation();
            this.probDatabase = probDatabase;
            this.queryString = StandardizeQuery(queryString);
            this.flagNaturalJoin = false;
        }

        #region new
        private static string StandardizeQuery(string queryString)
        {
            try
            {
                string result = "";
                string S = queryString;
                for (int i = 0; i < S.Length; i++)
                    if (S[i] == ' ')
                    {
                        if (S[i - 1] != ' ')
                            result += S[i];
                    }
                    else
                        result += S[i];

                result = result.Replace("\n", " ");
                return result.ToLower();
            }
            catch (Exception)
            {
                return null;
            }

        }
        private ProbRelation Descartes()
        {
            ProbRelation relation = new ProbRelation();

            GenerateNewRelation(relation, this.selectedRelations[0]);
            GenerateNewRelation(relation, this.selectedRelations[1]);

            JoinTwoRelation(relation, this.selectedRelations[0], this.selectedRelations[1]);

            for (int i = 2; i < this.selectedRelations.Count; i++) 
            {
                GenerateNewRelation(relation, this.selectedRelations[i]);
                var tmpRelation = relation;
                JoinTwoRelation(relation, tmpRelation, this.selectedRelations[i]);
            }
            
            return relation;
        }

        private static void JoinTwoRelation(ProbRelation relation, ProbRelation relation1, ProbRelation relation2)
        {
            foreach (ProbTuple tupleOne in relation1.tuples)
            {
                foreach (ProbTuple tupleTwo in relation2.tuples)
                {
                    ProbTuple value = new ProbTuple();
                    value.Triples.AddRange(tupleOne.Triples);
                    value.Triples.AddRange(tupleTwo.Triples);
                    relation.tuples.Add(value);
                }
            }
        }

        private void NatureJoinTwoRelation(ProbRelation relation, ProbRelation relationInput)
        {
            var model =  joinRelationModel.FirstOrDefault(x=>x.RelationName == relationInput.RelationName || x.AcronymRelationName == relationInput.RelationName);

            if(model == null)
            {
                var tmpRelation = relation;
                foreach (ProbTuple tupleOne in tmpRelation.tuples)
                {
                    foreach (ProbTuple tupleTwo in relationInput.tuples)
                    {
                        ProbTuple value = new ProbTuple();
                        value.Triples.AddRange(tupleOne.Triples);
                        value.Triples.AddRange(tupleTwo.Triples);
                        relation.tuples.Add(value);
                    }
                }
            }
            else
            {
                var listValue = model.ConditionKey.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);

                switch (model.Operation)
                {
                    case "inner join":
                        var tmpRelation = relation;

                        var indexOne = IndexOfAttribute(listValue[0].Trim(), relation);
                        var indexTwo = IndexOfAttribute(listValue[0].Trim(), relation);

                        foreach (ProbTuple tupleOne in tmpRelation.tuples)
                        {
                            foreach (ProbTuple tupleTwo in relationInput.tuples)
                            {
                                ProbTuple value = new ProbTuple();
                                value.Triples.AddRange(tupleOne.Triples);
                                value.Triples.AddRange(tupleTwo.Triples);
                                relation.tuples.Add(value);
                            }
                        }
                        break;
                    case "left join":
                        break;
                    case "right join":
                        break;
                    case "full outer join":
                        break;
                    case "natural join in":
                        break;
                    case "natural join ig":
                        break;
                    case "natural join me":
                        break;
                    default:
                        return;
                }


            }


            return;
        }

        public int IndexOfAttribute(string attribute, ProbRelation probRelation)
        {
            string value = attribute.Trim().ToLower();
            int indexAttributeS = -1;
            if (value.IndexOf(".") != -1)
            {
                string[] arr = value.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
                int count = 0;
                foreach (string item in probRelation.ListRenameRelation)
                {
                    if (item != arr[0])
                        count++;
                }
                if (count == 2)
                {
                    MessageError = String.Format("The multi-part identifier {0} could not be bound.", value);
                    return -1;
                }

                for (int i = 0; i < probRelation.Scheme.Attributes.Count; i++)
                {
                    if (value == probRelation.Scheme.Attributes[i].AttributeName.ToLower())
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
                for (int i = 0; i < probRelation.Scheme.Attributes.Count; i++)
                {
                    string[] arr = probRelation.Scheme.Attributes[i].AttributeName.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
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

        private void GenerateNewRelation(ProbRelation relation, ProbRelation relationInput)
        {
            relation.ListRenameRelation.Add(relationInput.RelationName);
            foreach (ProbAttribute attr in relationInput.Scheme.Attributes)
            {
                if (attr.AttributeName.IndexOf(".") == -1)
                    attr.AttributeName = relationInput.RelationName + "." + attr.AttributeName;
            }
            relation.Scheme.Attributes.AddRange(relationInput.Scheme.Attributes);
        }

        private void GenerateNewRelationV2(ProbRelation relation, ProbRelation relationInput)
        {
            relation.ListRenameRelation.Add(relationInput.RelationName);
            foreach (ProbAttribute attr in relationInput.Scheme.Attributes)
            {
                if (attr.AttributeName.IndexOf(".") == -1)
                    attr.AttributeName = relationInput.RelationName + "." + attr.AttributeName;
            }
            relation.Scheme.Attributes.AddRange(relationInput.Scheme.Attributes);
        }


        private static ProbTriple JoinTwoTriple(ProbTriple tripleOne, ProbTriple tripleTwo, ProbAttribute attribute, string OperationNaturalJoin)
        {
            ProbTriple triple = new ProbTriple();

            for (int i = 0; i < tripleOne.Value.Count; i++)
            {
                for (int j = 0; j < tripleTwo.Value.Count; j++)
                {
                    if (SelectCondition.EQUAL(tripleOne.Value[i].ToString().Trim(), tripleTwo.Value[j].ToString().Trim(), attribute.Type.DataType))
                    {
                        triple.Value.Add(tripleOne.Value[i]);
                    }
                }
            }

            if(triple.Value.Count > 0)
            {
                switch (OperationNaturalJoin)
                {
                    case "in":
                        triple.MinProb = tripleOne.MinProb * tripleTwo.MinProb;
                        triple.MaxProb = tripleOne.MaxProb * tripleTwo.MaxProb;
                        break;

                    case "ig":
                        triple.MinProb = Math.Max(0, (tripleOne.MinProb + tripleTwo.MinProb) - 1);
                        triple.MaxProb = Math.Min(tripleOne.MaxProb, tripleTwo.MaxProb);
                        break;

                    case "me":
                        triple.MinProb = 0;
                        triple.MaxProb = 0;
                        break;
                    default: break;
                }
            }
            return triple.Value.Count <= 0 ? null : triple;
        }
        private ProbRelation NaturalJoinV2()
        {
            ProbRelation relation = new ProbRelation();

            //var conditionString = joinRelationModel.Where(x=>x.RelationName.Contains(selectedRelations[1].RelationName)).FirstOrDefault();

            GenerateNewRelationV2(relation, selectedRelations[0]);

            for (int i = 1; i < this.selectedRelations.Count; i++)
            {
                GenerateNewRelationV2(relation, selectedRelations[i]);

            }

            return relation;
        }

        private ProbRelation NaturalJoin()
        {
            ProbRelation relation = Descartes();
            List<int> indexsRemove = new List<int>();

            for (int i = 0; i < relation.Scheme.Attributes.Count - this.selectedRelations[1].Scheme.Attributes.Count; i++)
            {
                for (int j = this.selectedRelations[1].Scheme.Attributes.Count; j < relation.Scheme.Attributes.Count; j++)
                {
                    if (i != j && relation.Scheme.Attributes[i].Type.DataType == relation.Scheme.Attributes[j].Type.DataType)
                    {
                        string attributeOne = relation.Scheme.Attributes[i].AttributeName.Substring(relation.Scheme.Attributes[i].AttributeName.IndexOf(".") + 1);
                        string attributeTwo = relation.Scheme.Attributes[j].AttributeName.Substring(relation.Scheme.Attributes[j].AttributeName.IndexOf(".") + 1);

                        if (attributeOne.Equals(attributeTwo, StringComparison.CurrentCultureIgnoreCase))
                        {
                            indexsRemove.Add(j);

                            for (int k = relation.tuples.Count - 1; k >= 0; k--)
                            {
                                ProbTriple triple = JoinTwoTriple(relation.tuples[k].Triples[i], relation.tuples[k].Triples[j], relation.Scheme.Attributes[i], this.OperationNaturalJoin);
                                if (triple != null)
                                {
                                    relation.tuples[k].Triples[i] = triple;
                                    relation.tuples[k].Triples[j] = triple;
                                }
                                else
                                {
                                    relation.tuples.RemoveAt(k);
                                }
                            }
                        }
                    }
                }
            }

            for (int i = 0; i < indexsRemove.Count; i++)
            {

                foreach (ProbTuple tuple in relation.tuples)
                {
                    tuple.Triples.RemoveAt(indexsRemove[i]);
                }
                relation.Scheme.Attributes.RemoveAt(indexsRemove[i]);
                this.selectedAttributes.RemoveAt(indexsRemove[i]);
            }


            OperationNaturalJoin = string.Empty;
            flagNaturalJoin = false;
            return relation;
        }
        

        private static ProbRelation getRelationBySelectAttribute(ProbRelation probRelation, List<ProbAttribute> attributes)
        {
            ProbRelation relation = new ProbRelation();
            relation.RelationName = probRelation.RelationName;

            List<int> indexs = new List<int>();
            List<int> indexRemove = new List<int>();
            foreach (ProbAttribute attr in attributes)
            {
                for (int i = 0; i < probRelation.Scheme.Attributes.Count; i++)
                {
                    if (probRelation.Scheme.Attributes[i].AttributeName.Trim().ToLower() == attr.AttributeName.Trim().ToLower())
                    {
                        indexs.Add(i);
                        break;
                    }
                }
            }



            foreach (ProbTuple item in probRelation.tuples)
            {
                ProbTuple tuple = new ProbTuple();
                for (int i = 0; i < indexs.Count; i++)
                {
                    tuple.Triples.Add(item.Triples[indexs[i]]);
                }
                relation.tuples.Add(tuple);
            }


            relation.Scheme.Attributes = attributes;

            return relation;
        }

        internal bool ExcuteQueryV2()
        {
            try
            {
                if (!QueryAnalyzeV2()) return false;

                if (this.selectedRelations.Count == 1)
                {
                    foreach (ProbAttribute attr in this.selectedRelations[0].Scheme.Attributes)
                    {
                        if (!attr.AttributeName.Contains("."))
                            attr.AttributeName = String.Format("{0}.{1}", this.selectedRelations[0].RelationName, attr.AttributeName);
                    }
                }
                else
                {
                    if (flagNaturalJoin != true)
                        this.selectedRelations[0] = Descartes();
                    else
                        this.selectedRelations[0] = NaturalJoin();
                }

                if (!this.queryString.Contains(Common.Where))
                {
                    this.relationResult = getRelationBySelectAttributeV2(this.selectedRelations[0], this.selectedAttributes);
                    return true;
                }
                else
                {
                    SelectCondition Condition = new SelectCondition(this.selectedRelations[0], this.conditionString);

                    Condition.ProcessConditionString();

                    if (!string.IsNullOrEmpty(Condition.MessageError))
                    {
                        MessageError = Condition.MessageError;
                        return false;
                    }

                    foreach (ProbTuple tuple in this.selectedRelations[0].tuples)
                        if (Condition.Satisfiedv2(tuple))
                            this.relationResult.tuples.Add(tuple);

                    if (Condition.MessageError != string.Empty)
                    {
                        this.MessageError = Condition.MessageError;
                        return false;
                    }

                    if (Condition.conditionString == string.Empty)
                    {
                        this.MessageError = Condition.MessageError;
                        return false;
                    }

                    this.relationResult.Scheme = this.selectedRelations[0].Scheme;
                    this.relationResult = getRelationBySelectAttribute(this.relationResult, this.selectedAttributes);
                }
            }
            catch
            {
                return false;
            }
            return true;
        }

        internal bool QueryAnalyzeV2()
        {
            try
            {
                string s = this.queryString;
                //Kiểm tra câu truy vấn có hợp lệ
                if (!this.CheckStringQueryV2(s))
                {
                    return false;
                }

                //Get All Relation
                this.selectedRelations = GetAllRelationV2(s);
                if (this.selectedRelations == null)
                {
                    return false;
                }

                //Get All Attribute
                this.selectedAttributes = GetAttributeV2(s);
                if (this.selectedAttributes == null)
                    return false;

                this.conditionString = GetConditionV2(s);

                return true;

            }
            catch (Exception)
            {
                return false;
            }
        }

        internal bool CheckStringQueryV2(string query)
        {
            try
            {
                var indexSelect = query.IndexOf(Common.Select);
                var indexFrom = query.IndexOf(Common.From);
                var indexWhere = query.IndexOf(Common.Where);

                if (indexSelect == -1 || indexFrom == -1)
                {
                    MessageError = "Syntax Error! The keyword must theo order 'Select From Where' ";
                    return false;
                }

                if (indexSelect > indexFrom || (indexWhere != -1 && indexFrom > indexWhere))
                {
                    MessageError = "Syntax Error! The keyword must theo order 'Select From Where' ";
                    return false;
                }

                var regex = new Regex("^[a-zA-Z *,.]*$");

                var subQuery = query;

                if (query.Contains(Common.Where))
                {
                    subQuery = query.Substring(query.IndexOf("select") + 6, query.IndexOf("where") - 1);
                }

                if (!regex.IsMatch(subQuery))
                {
                    MessageError = "Error: Do not input the special character in query statement";
                    return false;
                }

                return true;

            }
            catch
            {
                return false;
            }

        }

        private List<ProbRelation> GetAllRelationV2(string valueString)
        {
            int posOne;
            int posTwo;
            string relationsString = string.Empty;
            List<string> listOfRelationName = this.probDatabase.ListOfRelationNameToLower();
            string[] seperator = { "," };
            string[] relations;
            List<ProbRelation> probRelations = new List<ProbRelation>();

            //////////////////////// Get Relations ///////////////////////
            posOne = valueString.IndexOf(Common.From) + 4;

            if (!valueString.Contains(Common.Where))
                posTwo = valueString.Length - 1;
            else
                posTwo = valueString.IndexOf(Common.Where) - 1;

            relationsString = valueString.Substring(posOne, posTwo - posOne + 1);   // Get Relation in the Query Text     

            if (relationsString.Trim().Length <= 0)
            {
                MessageError = "No relation exists in the query !";
                return null;
            }

            if (relationsString.Contains(","))
            {
                relations = relationsString.Split(seperator, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                if (relationsString.Contains(Common.NaturalJoinIn) || relationsString.Contains(Common.NaturalJoinIg) || relationsString.Contains(Common.NaturalJoinMe))
                {
                    relations = new string[2];

                    if (relationsString.Contains(Common.NaturalJoinIn))
                    {
                        relations[0] = relationsString.Substring(0, relationsString.IndexOf(Common.NaturalJoinIn)).Trim();
                        relations[1] = relationsString.Substring(relationsString.IndexOf(Common.NaturalJoinIn) + 16).Trim();
                        OperationNaturalJoin = "in";
                    }
                    else if (relationsString.Contains(Common.NaturalJoinIg))
                    {
                        relations[0] = relationsString.Substring(0, relationsString.IndexOf(Common.NaturalJoinIg)).Trim();
                        relations[1] = relationsString.Substring(relationsString.IndexOf(Common.NaturalJoinIg) + 16).Trim();
                        OperationNaturalJoin = "ig";
                    }
                    else
                    {
                        relations[0] = relationsString.Substring(0, relationsString.IndexOf(Common.NaturalJoinMe)).Trim();
                        relations[1] = relationsString.Substring(relationsString.IndexOf(Common.NaturalJoinMe) + 16).Trim();
                        OperationNaturalJoin = "me";
                    }
                    flagNaturalJoin = true;
                }
                else if(relationsString.Contains(Common.InnerJoin) || relationsString.Contains(Common.LeftJoin) || relationsString.Contains(Common.ReightJoin) || relationsString.Contains(Common.FullJoin))
                {
                    var regexRelation1 = new Regex(@"^(?<relationOne>([\w\.\-]+)(?<acronym>[ ][\w\-]+)?)");
                    var regex = new Regex(@"(?<operation>([ ][i][n][n][e][r][ ][j][o][i][n][ ])|([ ][l][e][f][t][ ][j][o][i][n][ ])|([ ][r][i][g][h][t][ ][j][o][i][n][ ])|([ ][f][u][l][l][ ][o][u][t][e][r][ ][j][o][i][n][ ]))(?<relationTwo>(?<relationName>[\w\-]+)(?<acronym2>[ ][\w\-]+)?)(?<on>[ ][o][n][ ])(?<condition>[\w\.\-]+[=][\w\.\-]+)");

                    var mathOne = regexRelation1.Match(relationsString);
                    var mathTwo = regex.Matches(relationsString);

                    relations = new string[mathTwo.Count+1];
                    relations[0] = mathOne.ToString();
                    var joinRelationModelOne = new JoinRelationModel
                    {
                        RelationName = mathOne.Groups["relationOne"].ToString().Trim(),
                        AcronymRelationName = mathOne.Groups["acronym"].ToString().Trim()
                    };

                    relationsString = relationsString.Replace(mathOne.ToString(), "");

                    for (int i=0; i < mathTwo.Count; i++)
                    {
                        var joinRelationModel = new JoinRelationModel
                        {
                            RelationName = mathTwo[i].Groups["relationName"].ToString(),
                            ConditionKey = mathTwo[i].Groups["condition"].ToString(),
                            Operation = mathTwo[i].Groups["operation"].ToString(),
                            AcronymRelationName = mathTwo[i].Groups["acronym2"].ToString(),
                            Stage = i 
                        };

                        relationsString = relationsString.Replace(mathTwo[i].ToString(), "");
                        relations[i + 1] = mathTwo[i].Groups["relationTwo"].ToString();
                    }

                    if (!string.IsNullOrEmpty(relationsString.Trim()))
                    {
                        MessageError = "Incorrect syntax.";
                        return null;
                    }
                }
                else
                {
                    relations = new string[1];
                    relations[0] = relationsString;
                }
            }

            for (int i = 0; i < relations.Length; i++)
            {
                string[] listTmp = relations[i].Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                if (!listOfRelationName.Contains(listTmp[0].ToLower()))
                {
                    MessageError = String.Format("Invalid relation name '{0}'.", listTmp[0]);
                    return null;
                }

                ProbRelation tmp = this.probDatabase.Relations.SingleOrDefault(c => c.RelationName.ToLower().Equals(listTmp[0], StringComparison.OrdinalIgnoreCase));

                ProbRelation rela = new ProbRelation();
                if (listTmp.Length == 2)
                {
                    rela.RelationName = listTmp[1];
                }
                else
                    rela.RelationName = tmp.RelationName;

                rela.Scheme = new ProbScheme(-1, tmp.Scheme.SchemeName, tmp.Scheme.Attributes);

                foreach (ProbTuple item in tmp.tuples)
                {
                    ProbTuple tuple = new ProbTuple(item);
                    rela.tuples.Add(tuple);
                }

                probRelations.Add(rela);
            }
            //if (probRelations.Count == 2)
            //{
            //    if (probRelations[0].RelationName == probRelations[1].RelationName)
            //    {
            //        MessageError = String.Format("The correlation name '{0}' is specified multiple times in a FROM clause.", probRelations[0].RenameRelationName);
            //        return null;
            //    }
            //}

            return probRelations;

        }

        private List<ProbAttribute> GetAttributeV2(string valueString)
        {
            List<ProbAttribute> listProbAttribute = new List<ProbAttribute>();
            //////////////////////// Get Attributes //////////////////////
            int posOne, posTwo, posThree;


            // * là chọn tất cả các thuộc tính
            if (valueString.Contains("*"))
            {
                posOne = valueString.IndexOf("*");  // start postion of attributes
                posTwo = valueString.IndexOf("from ") - 1;
                posThree = valueString.IndexOf(Common.Select) + 6;

                if (posOne > posTwo)
                {
                    MessageError = "Incorrect syntax near 'from'.";
                    return null;
                }

                if (posOne < valueString.IndexOf(Common.Select))
                {
                    MessageError = "Incorrect syntax near 'select'.";
                    return null;
                }

                if (valueString.Contains(Common.Where) && posOne > valueString.IndexOf(Common.Where))
                {
                    MessageError = "Incorrect syntax near 'where'.";
                    return null;
                }

                if (posOne != valueString.LastIndexOf("*"))
                {
                    MessageError = "Incorrect syntax near 'select'.";
                    return null;
                }

                // end postion of attributes
                string attributes = valueString.Substring(posOne, posTwo - posOne + 1);
                string attributesBeforeStar = valueString.Substring(posThree, posOne - posThree + 1);

                // Nếu như phia sau dấu * có bất kì kí tự nào thì sẽ thông báo lỗi
                if (attributes.Trim().Length > 1 || attributesBeforeStar.Trim().Length > 1)
                {
                    MessageError = "Incorrect syntax near 'select'.";
                    return null;
                }

                // thực hiện sao chép toàn bộ thuộc tính của các quan hệ vào danh sách thuộc tính chọn
                for (int i = 0; i < this.selectedRelations.Count; i++)
                {
                    foreach (ProbAttribute attr in this.selectedRelations[i].Scheme.Attributes)
                    {
                        attr.AttributeName = String.Format("{0}.{1}", this.selectedRelations[i].RelationName, attr.AttributeName);
                        listProbAttribute.Add(attr);
                    }
                }

                return listProbAttribute;

            }
            else // ngược lại là xuất theo thuộc tính chỉ định
            {
                posOne = valueString.IndexOf("select") + 6;                                                   // start postion of attributes
                posTwo = valueString.IndexOf("from ") - 1;                                                    // end postion of attributes

                string attributes = valueString.Substring(posOne, posTwo - posOne + 1);

                //kiểm tra cú pháp của chuổi thuộc tính
                if (!QueryExecution.CheckStringAttributeV2(attributes))
                {
                    MessageError = "Incorrect syntax near 'select'.";
                    return null;
                }
                else
                {
                    string[] seperator = { "," };
                    string[] attribute = attributes.Split(seperator, StringSplitOptions.RemoveEmptyEntries); // split thành mảng các thuộc tính                    

                    foreach (string str in attribute)
                    {
                        if (!str.Contains("."))
                        {
                            string attributeName = str.Trim();
                            int countOne = 0;
                            int countSameAttribute = 0;
                            foreach (ProbRelation relation in this.selectedRelations)
                            {
                                List<string> listOfAttributeName = relation.Scheme.ListOfAttributeNameToLower();
                                if (listOfAttributeName.Contains(attributeName.ToLower()))
                                {
                                    ProbAttribute attr = new ProbAttribute(relation.Scheme.Attributes[listOfAttributeName.IndexOf(attributeName)]);
                                    attr.AttributeName = String.Format("{0}.{1}", relation.RelationName, attr.AttributeName);
                                    listProbAttribute.Add(attr);
                                    countSameAttribute++;
                                }
                                else
                                {
                                    countOne++;
                                }
                            }

                            if (countOne == this.selectedRelations.Count)
                            {
                                MessageError = String.Format(" Invalid attribute name '{0}'.", attributeName);
                                return null;
                            }

                            if (countSameAttribute == this.selectedRelations.Count && this.selectedRelations.Count >= 2)
                            {
                                MessageError = String.Format(" Ambiguous attribute name '{0}'.", attributeName);
                                return null;
                            }
                        }
                        else
                        {
                            string[] array = str.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
                            if (array.Length != 2)
                            {
                                MessageError = "Incorrect syntax near the keyword 'select'.";
                                return null;
                            }

                            ProbRelation relation = this.selectedRelations.SingleOrDefault(c => c.RelationName.Trim() == array[0].Trim());

                            if (relation == null)
                            {
                                MessageError = String.Format("The multi-part identifier '{0}' could not be bound.", str);
                                return null;
                            }

                            ProbAttribute attr = new ProbAttribute(relation.Scheme.Attributes.SingleOrDefault(c => c.AttributeName.Trim().ToLower() == array[1].Trim()));
                            attr.AttributeName = String.Format("{0}.{1}", relation.RelationName, attr.AttributeName);

                            if (attr == null)
                            {
                                MessageError = "Invalid attribute name '" + array[1] + "'.";
                                return null;
                            }

                            listProbAttribute.Add(attr);
                        }
                    }

                    return listProbAttribute;
                }
            }
        }

        private static bool CheckStringAttributeV2(string stringAttribute)
        {

            string subString = stringAttribute;

            if (subString.Trim().Length <= 0)
                return false;

            if (subString.Contains(",,"))
                return false;

            if (subString.LastIndexOf(",") == subString.Length - 1)
            {
                return false;
            }

            return true;
        }

        private static string GetConditionV2(string valueString)
        {

            string conditionString = string.Empty;
            int posOne;

            ///////////////////// Get Select Condition /////////////////
            if (valueString.Contains("where "))
            {
                posOne = valueString.IndexOf("where") + 5;
                conditionString = valueString.Substring(posOne);   // Get Select Condition in the Query Text
            }

            return conditionString;
        }

        private static ProbRelation getRelationBySelectAttributeV2(ProbRelation probRelation, List<ProbAttribute> attributes)
        {
            ProbRelation relation = new ProbRelation();
            relation.RelationName = probRelation.RelationName;

            List<int> indexs = new List<int>();
            List<int> indexRemove = new List<int>();

            for (int i = 0; i < probRelation.Scheme.Attributes.Count; i++)
            {
                if (attributes.Any(x => x.AttributeName.Trim().ToLower().Contains(probRelation.Scheme.Attributes[i].AttributeName.Trim().ToLower())))
                {
                    indexs.Add(i);
                }
            }

            foreach (ProbTuple item in probRelation.tuples)
            {
                ProbTuple tuple = new ProbTuple();
                for (int i = 0; i < indexs.Count; i++)
                {
                    tuple.Triples.Add(item.Triples[indexs[i]]);
                }
                relation.tuples.Add(tuple);
            }

            relation.Scheme.Attributes = attributes;

            return relation;
        }
        #endregion
    }
}
