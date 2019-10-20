using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace PRDB_Sqlite.BLL
{
    public class ProbTriple
    {
        #region Properties
        // Tập các giá trị
        public List<string> Value { get; set; }
      
        // Tập xác suất cận dưới
        public double MinProb { get; set; }
     
        // Tập xác suất cận trên
        public double MaxProb { get; set; }

        #endregion
        #region Methods
        public ProbTriple()
        {
            this.Value = new List<string>();
            this.MinProb = 1;
            this.MaxProb = 1;
           
        }

        // Tạo bộ ba xác suất từ chuỗi text
        public ProbTriple(string value)
        {
            try
            {
                this.Value = new List<string>();
                this.MinProb = 1;
                this.MaxProb = 1;

                if(!value.Contains("{") && !value.Contains("}") && !value.Contains("[") && !value.Contains("]"))
                {
                   
                    if (Value.Contains(","))
                    {
                        string[] seperator = { "," };
                        string[] listValue = value.Split(seperator, StringSplitOptions.RemoveEmptyEntries);

                        foreach (var item in listValue)
                        {
                            this.Value.Add(item);
                        }
                    }
                    else
                    {
                        Value.Add(value);
                    }
                }
                else
                {
                    int j1, j2, j3, j4, j5;
                    j1 = value.IndexOf('{');
                    j2 = value.IndexOf('}');
                    j3 = value.IndexOf('[');
                    j4 = value.LastIndexOf(',');
                    j5 = value.IndexOf(']');

                    this.MinProb = double.Parse(value.Substring(j3 + 1, j4 - j3 - 1));
                    this.MaxProb = double.Parse(value.Substring(j4 + 1, j5 - j4 - 1));
                    var valueString = value.Substring(j1 + 1, j2 - j1 - 1);

                    string[] seperator = { "," };
                    string[] listValue = valueString.Split(seperator, StringSplitOptions.RemoveEmptyEntries);

                    foreach(var item in listValue)
                    {
                        this.Value.Add(item.Trim());
                    }
                    
                } 
            }
            catch
            {
               
            }
        }

        public ProbTriple(List<string> values, double maxProb, double minProb)
        {
            try
            {
                this.Value = new List<string>();
                this.MinProb = minProb;
                this.MaxProb = maxProb;

                Value.AddRange(values);
            }
            catch
            {

            }
        }

        public ProbTriple(ProbTriple triple)
        {
            // TODO: Complete member initialization
            this.Value = new List<string>();
            this.MinProb = triple.MinProb;
            this.MaxProb = triple.MaxProb;

            foreach (var item in triple.Value)
            {
                this.Value.Add(item);
            }
        }

        // Xuất bộ ba xác suất ra chuỗi giá trị
        public string GetStrValue()
        {
            string strValue = "";
            foreach (var item in Value)
            {
                strValue += item;
                strValue += ", ";
            }
            if (strValue.EndsWith(", "))
            {
                strValue = strValue.Remove(strValue.Length - 2);
            }
            if (this.MinProb != 1)
            {
                strValue = "{"+strValue+"}[ " + this.MinProb.ToString() + ", " + this.MaxProb + "]";

            }
            return strValue;
        }

        public bool isProbTripleValue(string value)
        {
            if (!value.Contains("{") && !value.Contains("}") && !value.Contains("[") && !value.Contains("]"))
                return true;
            else
            {
                if (!value.Contains("{") || !value.Contains("}") || !value.Contains("[") || !value.Contains("]"))
                    return false;
                return true;
            }
        }
        #endregion
    }
}
