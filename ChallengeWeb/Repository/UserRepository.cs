using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;
using Challenge;

namespace ChallengeWeb.Repository
{
    public interface IUserRepository
    {
        User FetchUser(string username, bool addIfMissing = false);
        User AddUser(string username);
        User AddUser(User usr);
    }

    public class UserRepository : IUserRepository
    {
        //note: i'm making the backing Dict static, so it will live beyond the lifetime of the repo instance,
        //which is not strictly necessary, but it makes it behave a little more like a persistent store
        private static readonly ConcurrentDictionary<string, User> _users = new ConcurrentDictionary<string, User>(StringComparer.InvariantCultureIgnoreCase);
        private static Random _rand = new Random();
        private static readonly string[] _colors = { "Red", "Green", "Blue", "Brown", "Violet", "Yellow", "Purple", "Chartreuse", "Imaginary Orange" };

        public UserRepository()
        { }

        public User FetchUser(string username, bool addIfMissing)
        {
            if (addIfMissing)
                return _users.GetOrAdd(username, AddUser);

            if (_users.TryGetValue(username, out User u))
                return u;
            else
                return null;

        }

        public User AddUser(string username)
        {
            User u = RandomUser(username);
            return AddUser(u);
        }

        public User AddUser(User usr)
        {
            _users[usr.Username] = usr;
            return FetchUser(usr.Username, false);
        }

        private static User RandomUser(string username)
        {
            var u = new User
            {
                Username = username,
                FavoriteColor = RandomColor(),
                DateCreated = RandomDate(),
                TimeZone = RandomTimeZone()
            };
            var permissions = RandomPermissions(10);
            foreach (var p in permissions)
            {
                u.Permissions[p] = true;
            }
            return u;
        }

        private static List<PermissionIndex> RandomPermissions(int num)
        {
            List<PermissionIndex> selected = new List<PermissionIndex>(num);
            var vals = Enum.GetValues(typeof(PermissionIndex));

            for (int i = 0; i < num; i++)
            {
                int idx = _rand.Next(0, vals.Length);
                selected.Add((PermissionIndex)vals.GetValue(idx));
            }
            return selected;

        }

        private static DateTime RandomDate()
        {
            int dayOfYear = _rand.Next(0, DateTime.Now.DayOfYear);
            return new DateTime(2020, 1, 1).AddDays(dayOfYear);
        }

        
        private static string RandomColor()
        {            
            int idx = _rand.Next(0, _colors.Length);
            return _colors[idx];
        }

        private static string RandomTimeZone()
        {
            var sysZones = TimeZoneInfo.GetSystemTimeZones();
            int idx = _rand.Next(0, sysZones.Count);
            return sysZones[idx].Id;
        }
    }
}
