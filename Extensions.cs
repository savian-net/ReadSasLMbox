using System.Text.RegularExpressions;

namespace ReadSasLMbox
{
    public static class Extensions
    {
        public static List<string> SplitAndKeepDelimiter(this string input, string pattern)
        {
            var rx = new Regex(pattern, RegexOptions.Multiline);
            var matches = rx.Matches(input);
            var recs = new List<string>();
            int i = 1;
            foreach (Match m in matches)
            {
                var msg = i < matches.Count ?
                        input.Substring(m.Index, matches[i].Index - m.Index) :
                        input.Substring(m.Index)
                    ;
                recs.Add(msg);
                i++; 
            }
            return recs;
        }
    }
}
