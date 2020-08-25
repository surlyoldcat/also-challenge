using System;
using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;

namespace AE.CoreUtility
{
    [DebuggerStepThrough]
    public static class DateTimeEx
    {
        public const string c_serverculture = "en";
        public const string c_iso8601_date = "yyyy-MM-dd";
        public const string c_iso8601_time = "HH:mm:ss.fffZ";
        public const string c_iso8601_datetime = "yyyy-MM-ddTHH:mm:ss.fffZ";
        public const string c_iso8601_sitetime = "HH:mm:ss.fff";
        public const string c_iso8601_sitedatetime = "yyyy-MM-ddTHH:mm:ss.fff";
        public const string c_iso8601_datetimenoms = "yyyy-MM-ddTHH:mm:ssZ";
        public const string c_timespan = "c"; // -d.hh:mm:ss.fffffff
        public const string c_simpledateformat = "yyyyMMddHHmm";
        public const string c_simpledateformatsecs = "yyyyMMddHHmmss";
        public const string c_datetimeroundtripoffset = "yyyy-MM-ddTHH:mm:ss.fffzzz";
        public const string c_datetimeroundtripnomsoffset = "yyyy-MM-ddTHH:mm:sszzz";
        public const string c_datetimeroundtripnoms = "yyyy-MM-ddTHH:mm:ssK";
        public const long s_ECMAMSecs = 0x001EB208C2DC0000;
        public readonly static DateTime s_Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public readonly static DateTime s_JSMin = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public readonly static DateTime s_Y2K = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public readonly static DateTime s_ECMAMin = s_Epoch.AddMilliseconds(Math.Max(-s_ECMAMSecs, (long)(DateTime.MinValue - s_Epoch).TotalMilliseconds));
        public readonly static DateTime s_ECMAMax = s_Epoch.AddMilliseconds(Math.Min(s_ECMAMSecs, (long)(DateTime.MaxValue - s_Epoch).TotalMilliseconds) - 1);
        public readonly static long s_TicksToEpoch = s_Epoch.Ticks;

        public readonly static DateTime[] Empty = new DateTime[0];
        public readonly static TimeSpan[] EmptyTime = new TimeSpan[0];

        public const string c_timeRegEx = @"((?<hr>[0-9]{1,2})(-|:)((?<mn>[0-9]{1,2})((-|:)((?<sc>[0-9]{1,2})(\.(?<ms>[0-9]{1,7}))?(?<tz>(Z|-[0-9]{2}:[0-9]{2}))?)?)?)?(\s*(?<ap>(A|P|a|p)(m|M)?)\s*)?)?"; // HH:mm:ss.fffffffzzz
        public const string c_datesearchRegEx = @"(?<yr>[0-9]{1,4})((-|/|\\|\s)((?<mo>[0-9]{1,2})((-|/|\\|\s)((?<dy>[0-9]{1,2})((T|_|\s)" + c_timeRegEx + ")?)?)?)?)?"; // yyyy-MM-ddTHH:mm:ss.fffffffzzz
        public const string c_timezoneparts = @"^\(UTC(?<s>\+|-)0*(?<h>[0-9]{1,2})\:(?<m>[0-9]{1,2})\)\s*(?<z>.+$)";
        public readonly static Regex DateSearch = new Regex(c_datesearchRegEx);

        public const string c_timesearchRegEx = @"(?<ng>-)?((?<dy>[0-9]+)\.)?" + c_timeRegEx; // -d.HH:mm:ss.fffffffzzz
        public readonly static Regex TimeSearch = new Regex(c_timesearchRegEx);

        public readonly static Regex TimeZoneParts = new Regex(c_timezoneparts);

        public readonly static DateTimeFormatInfo ServerDateFormat = new CultureInfo(c_serverculture).DateTimeFormat;

        public static DateTime AsKind(this DateTime dt, DateTimeKind kind) { return DateTime.SpecifyKind(dt, kind); }
        /// <summary>
        /// Change the date time kind to utc, does not alter time value
        /// </summary>
        public static DateTime AsUTC(this DateTime dt) { return dt.AsKind(DateTimeKind.Utc); }
        /// <summary>
        /// Change the date time kind to unspecified, does not alter time value
        /// </summary>
        public static DateTime AsLogical(this DateTime dt) { return dt.AsKind(DateTimeKind.Unspecified); }

        public static bool IsValid(this DateTime dt, DateTime? min = null, DateTime? max = null) { return dt > (min ?? DateTime.MinValue) && dt < (max ?? DateTime.MaxValue); }

        public static DateTimeOffset FromServerLocal(string s, int offset) { return new DateTimeOffset(FromServerOffset(s).Ticks, new TimeSpan(offset, 0, 0)); }
        public static bool TryFromServerLocal(string s, int offset, out DateTimeOffset ret, DateTimeOffset def) { if (String.IsNullOrWhiteSpace(s)) { ret = def; return false; } try { ret = FromServerLocal(s, offset); return true; } catch { ret = def; return false; } }

        public static DateTimeOffset FromServerOffset(string s) { return DateTimeOffset.ParseExact(s?.ToUpper(), c_iso8601_datetime, ServerDateFormat, DateTimeStyles.AssumeUniversal); }
        public static bool TryFromServer(string s, out DateTimeOffset ret, DateTimeOffset def) { if (String.IsNullOrWhiteSpace(s)) { ret = def; return false; } try { ret = FromServerOffset(s); return true; } catch { ret = def; return false; } }

        public static bool TryFromServerRoundtrip(string s, out DateTimeOffset ret, DateTimeOffset def) { if (String.IsNullOrWhiteSpace(s)) { ret = def; return false; } return DateTimeOffset.TryParse(s, null, DateTimeStyles.RoundtripKind, out ret); }

        public static DateTimeOffset FromServerDate(string s) { return DateTimeOffset.ParseExact(s?.ToUpper(), c_iso8601_date, ServerDateFormat, DateTimeStyles.AssumeUniversal); }
        public static bool TryFromServerDate(string s, out DateTimeOffset ret, DateTimeOffset def) { if (String.IsNullOrWhiteSpace(s)) { ret = def; return false; } try { ret = FromServerDate(s); return true; } catch { ret = def; return false; } }

        public static TimeSpan FromServerSpan(string s) { return TimeSpan.ParseExact(s, c_timespan, ServerDateFormat); }
        public static bool TryFromServer(string s, out TimeSpan ret, TimeSpan def) { if (String.IsNullOrWhiteSpace(s)) { ret = def; return false; } try { ret = FromServerSpan(s); return true; } catch { ret = def; return false; } }

        public static DateTime NoMilliseconds(this DateTime dt) { return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Kind); }
        public static DateTime NoMillisecondsRound(this DateTime dt, double custom = double.NaN) { double secs = (!dt.IsValid()) ? 0 : dt.Millisecond / 1000D; return dt.NoMilliseconds().SafeAddSeconds(Math.Max((DateTime.MinValue - dt).TotalSeconds, (int)Math.Min((DateTime.MaxValue - dt).TotalSeconds, (!custom.IsValid()) ? (int)(secs + 0.5) : (secs >= custom) ? 1 : 0))); }

        public static DateTime NoSeconds(this DateTime dt) { return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, dt.Kind); }

        public static DateTime NoSecondsRound(this DateTime dt, double custom = double.NaN) { double secs = (!dt.IsValid()) ? 0 : dt.Second + (dt.Millisecond / 1000D); return dt.NoSeconds().SafeAddMinutes((!custom.IsValid()) ? (int)(secs / 60D + 0.5) : (secs >= custom) ? 1 : 0); }

        public static DateTime NoMinutes(this DateTime dt) { return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, dt.Kind); }

        public static DateTime ModMinutes(this DateTime dt, int mod = 15) { return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, (int)((double)dt.Minute / mod) * mod, 0, dt.Kind); }
        public static DateTime SafeAddMinutes(this DateTime dt, int mins, DateTime def = default(DateTime)) { return (!dt.IsValid()) ? def : ((mins <= 0 && DateTime.MinValue.AddMinutes(-mins) <= dt) || (mins >= 0 && DateTime.MaxValue.AddMinutes(-mins) >= dt)) ? dt.AddMinutes(mins) : def; }
        public static DateTime SafeAddSeconds(this DateTime dt, double secs, DateTime def = default(DateTime)) { return (!dt.IsValid()) ? def : ((secs <= 0 && DateTime.MinValue.AddSeconds(-secs) <= dt) || (secs >= 0 && DateTime.MaxValue.AddSeconds(-secs) >= dt)) ? dt.AddSeconds(secs) : def; }

        public static bool IsRoundtripDate(string s) { return s != null && Regex.IsMatch(s, @"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{0,7}((-|\+)[0-9]{2}:[0-9]{2})?"); }
        public static bool IsTimeSpan(string s) { return s != null && Regex.IsMatch(s, @"-?([0-9]+\.)?[0-9]{2}:[0-9]{2}:[0-9]{1,2}(\.[0-9]{7})?"); }
        public static bool IsIso8601Date(string s) { return s != null && Regex.IsMatch(s, @"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{0,7}Z"); }
        public static bool IsIso8601DateOnly(string s) { return s != null && Regex.IsMatch(s, @"[0-9]{4}-[0-9]{2}-[0-9]{2}"); }
        public static DateTime? GetDateTime(object val, bool parse = true, bool isutc = false) { DateTimeOffset? ret = GetDate(val, parse, isutc); return isutc ? ret?.UtcDateTime : ret?.DateTime; }
        public static DateTimeOffset? GetDate(object val) { return GetDate(val, true, false); }
        public static DateTimeOffset? GetDate(object val, bool parse, bool isutc) {
            if (val is TimeSpan?) return (isutc ? new DateTimeOffset(DateTime.Today.Ticks, TimeSpan.Zero) : new DateTimeOffset(DateTime.Today)) + ((TimeSpan?)val ?? TimeSpan.Zero);
            if (val is TimeSpan) return DateTimeOffset.MinValue + (TimeSpan)val;
            if (val is DateTimeOffset? && IOEx.IsNullable(val)) return (DateTimeOffset?)val;
            if (val is DateTimeOffset) return (DateTimeOffset)val;
            if (val is DateTime? && IOEx.IsNullable(val)) {
                DateTime? dt = (DateTime?)val;
                if (dt == null) return null;
                if (dt.Value <= DateTime.MinValue.AddHours(14)) return DateTimeOffset.MinValue;
                if (dt.Value >= DateTime.MaxValue.AddHours(-14)) return DateTimeOffset.MaxValue;
                return isutc ? new DateTimeOffset(dt.Value.Ticks, TimeSpan.Zero) : new DateTimeOffset(dt.Value);
            }
            if (val is DateTime) {
                DateTime dt = (DateTime)val;
                if (dt <= DateTime.MinValue.AddHours(14)) return DateTimeOffset.MinValue;
                if (dt >= DateTime.MaxValue.AddHours(-14)) return DateTimeOffset.MaxValue;
                return isutc ? new DateTimeOffset(dt.Ticks, TimeSpan.Zero) : new DateTimeOffset(dt);
            }
            string s = parse ? val?.ToString() : null;
            if (s == null) return null;
            DateTimeOffset ret;
            if (IsRoundtripDate(s) && TryFromServerRoundtrip(s, out ret, DateTimeOffset.MinValue)) return ret; // is roundtrip date
            if (IsIso8601Date(s) && ((isutc && TryFromServer(s, out ret, DateTimeOffset.MinValue)) || (!isutc && TryFromServerLocal(s, DateTimeOffset.Now.Offset.Hours, out ret, DateTimeOffset.MinValue)))) return ret; // is iso 8601 date
            if (IsIso8601DateOnly(s) && TryFromServerDate(s, out ret, DateTimeOffset.MinValue)) return ret; // is iso 8601 date only
            if (DateTimeOffset.TryParse(s, null, isutc ? DateTimeStyles.AssumeUniversal : DateTimeStyles.AssumeLocal, out ret)) return ret; // is formatted date
            return null;
        }
        public static TimeSpan? GetTime(object val) { return GetTime(val, true); }
        public static TimeSpan? GetTime(object val, bool parse) {
            if (val is TimeSpan? && IOEx.IsNullable(val)) return (TimeSpan?)val;
            if (val is TimeSpan) return (TimeSpan)val;
            if (val is long) return new TimeSpan((long)val);
            long? n = NumberEx.GetLong(val);
            if (n != null) return (n == 0) ? TimeSpan.Zero : new TimeSpan(n.Value);
            string s = parse ? val?.ToString() : null;
            if (s == null) return (TimeSpan?)null;
            TimeSpan ret;
            if (IsTimeSpan(s) && TryFromServer(s, out ret, TimeSpan.Zero)) return ret;
            if (TimeSpan.TryParse(s, out ret)) return ret;
            return ParseTime(s);
        }
        public static TimeSpan? ParseTime(string val) {
            if (String.IsNullOrWhiteSpace(val)) return null;
            string[] v = val.Split(StringEx.WhiteSpaceSplit, StringSplitOptions.RemoveEmptyEntries);
            double n = 0;
            bool nok = v.Length == 2 && NumberEx.TryFromServer(v[0], out n, 0.0);
            int span = (int)Math.Floor(n);
            DateTimeOffset now = DateTimeOffset.Now;
            string ts = v[v.Length - 1].ToLower();
            switch (ts) {
                case "past": case "min": return TimeSpan.MinValue;
                case "future": case "max": return TimeSpan.MaxValue;
                case "now": case "zero": case "0": return TimeSpan.Zero;
                case "millisecond": case "milliseconds": return new TimeSpan(0, 0, 0, 0, span);
                case "second": case "seconds": return new TimeSpan(0, 0, span);
                case "minute": case "minutes": return new TimeSpan(0, span, 0);
                case "hour": case "hours": return new TimeSpan(span, 0, 0);
                case "day": case "days": return new TimeSpan(span, 0, 0, 0);
                case "week": case "weeks": return new TimeSpan(span * 7, 0, 0, 0);
                case "month": case "months": return new TimeSpan((span * 30) + (int)Math.Floor(span * 10.5 / 24.0), (int)Math.Floor((span * 10.5) % 24.0), (int)Math.Floor((((span * 10.5) % 24.0) % 1) * 60), 0); // 30 days, 10.5 hours per month = 365.25 days
                case "year": case "years": return new TimeSpan((span * 365) + (int)Math.Floor(span / 4.0), (int)Math.Floor((span % 4.0) * 6), 0, 0); // 365.25 days in a year factoring in leap year
                default:
                    if (String.Compare(StringEx.Localize("past"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "past";
                    if (String.Compare(StringEx.Localize("min"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "min";
                    if (String.Compare(StringEx.Localize("future"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "future";
                    if (String.Compare(StringEx.Localize("max"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "max";
                    if (String.Compare(StringEx.Localize("now"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "now";
                    if (String.Compare(StringEx.Localize("zero"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "zero";
                    if (String.Compare(StringEx.Localize("millisecond"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "millisecond";
                    if (String.Compare(StringEx.Localize("milliseconds"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "milliseconds";
                    if (String.Compare(StringEx.Localize("minute"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "minute";
                    if (String.Compare(StringEx.Localize("hour"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "hour";
                    if (String.Compare(StringEx.Localize("day"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "day";
                    if (String.Compare(StringEx.Localize("week"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "week";
                    if (String.Compare(StringEx.Localize("weeks"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "weeks";
                    if (String.Compare(StringEx.Localize("month"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "month";
                    if (String.Compare(StringEx.Localize("months"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "months";
                    if (String.Compare(StringEx.Localize("year"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "year";
                    if (String.Compare(StringEx.Localize("years"), ts, StringComparison.OrdinalIgnoreCase) == 0) goto case "years";
                    return null;
            }
        }

        public static string ToJSDateStr(this DateTime dt) { return dt.IsValid() ? dt.ToString("MM/dd/yyyy HH:mm:ss") : String.Empty; }

        public readonly static TimeSpan c_onesecond = new TimeSpan(0, 0, 1);
        public readonly static TimeSpan c_fiveseconds = new TimeSpan(0, 0, 5);
        public readonly static TimeSpan c_oneminute = new TimeSpan(0, 1, 0);
        public readonly static TimeSpan c_fiveminutes = new TimeSpan(0, 5, 0);
        public readonly static TimeSpan c_fifteenminutes = new TimeSpan(0, 15, 0);
        public readonly static TimeSpan c_onehour = new TimeSpan(1, 0, 0);
        public readonly static TimeSpan c_oneday = new TimeSpan(1, 0, 0, 0);
        public readonly static TimeSpan c_31days = new TimeSpan(31, 0, 0, 0);
        public readonly static TimeSpan c_onedaydstsafe = new TimeSpan(1, 1, 0, 0);
        public readonly static TimeSpan c_fivedaydstsafe = new TimeSpan(5, 1, 0, 0);
        public readonly static TimeSpan c_oneweek = new TimeSpan(7, 0, 0, 0);
        public readonly static TimeSpan c_onemonth = new TimeSpan(30, 10, 30, 0);  // 30 days, 10.5 hours per month = 365.25 days
        public readonly static TimeSpan c_oneyear = new TimeSpan(365, 6, 0, 0);  // 365.25 days
        public const double c_daysinmonth = 30 + (10.5 / 24.0);  // 30 days, 10.5 hours per month = 365.25 days = 365.25 / 12
        public const double c_daysinquarter = c_daysinmonth * 3;
        public const double c_daysinyear = 365.25;  // 365.25 days counting leap year
        public const int c_maxdaysinyear = 366;
        public const int c_maxdaysinquarter = 92;
        public const int c_weeksinyear = 52;
        public const int c_maxdaysinmonth = 31;
        public const int c_monthsinyear = 12;
        public const int c_daysinweekend = 2;
        public const int c_weekdaysinweek = 5;
        public const int c_daysinweek = 7;
        public const int c_secsin5mins = 5 * 60;
        public const int c_secsin15mins = 15 * 60;
        public const int c_secsinhour = 60 * 60;
        public const int c_secsinday = 24 * c_secsinhour;
        public const int c_minsinday = 24 * 60;
        public const int c_minsinyear = (int)(c_minsinday * c_daysinyear);
        public static string ToTimeString(this TimeSpan ts) {
            if (ts <= TimeSpan.MinValue) return StringEx.Localize("min");
            if (ts >= TimeSpan.MaxValue) return StringEx.Localize("max");
            if (ts == TimeSpan.Zero) return StringEx.Localize("zero");

            double years = ts.TotalDays / c_daysinyear;
            int i = (int)Math.Floor(years);
            if (years == i) return String.Concat(i, ' ', (Math.Abs(i) > 1) ? StringEx.Localize("years") : StringEx.Localize("year"));

            double months = ts.TotalDays / c_daysinmonth;
            i = (int)Math.Floor(months);
            if (months == i) return String.Concat(i, ' ', (Math.Abs(i) > 1) ? StringEx.Localize("months") : StringEx.Localize("month"));

            double weeks = ts.TotalDays / 7;
            i = (int)Math.Floor(weeks);
            if (weeks == i) return String.Concat(i, ' ', (Math.Abs(i) > 1) ? StringEx.Localize("weeks") : StringEx.Localize("week"));

            i = (int)Math.Floor(ts.TotalDays);
            if (ts.TotalDays == i) return String.Concat(i, ' ', (Math.Abs(i) > 1) ? StringEx.Localize("days") : StringEx.Localize("day"));

            i = (int)Math.Floor(ts.TotalHours);
            if (ts.TotalHours == i) return String.Concat(i, ' ', (Math.Abs(i) > 1) ? StringEx.Localize("hours") : StringEx.Localize("hour"));

            i = (int)Math.Floor(ts.TotalMinutes);
            if (ts.TotalMinutes == i) return String.Concat(i, ' ', (Math.Abs(i) > 1) ? StringEx.Localize("minutes") : StringEx.Localize("minute"));

            i = (int)Math.Floor(ts.TotalSeconds);
            if (ts.TotalSeconds == i) return String.Concat(i, ' ', (Math.Abs(i) > 1) ? StringEx.Localize("seconds") : StringEx.Localize("second"));

            i = (int)Math.Floor(ts.TotalMilliseconds);
            return String.Concat(i, ' ', (Math.Abs(i) > 1) ? StringEx.Localize("milliseconds") : StringEx.Localize("millisecond"));
        }

    }
}
