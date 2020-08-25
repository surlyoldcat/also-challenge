using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
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

        /// <summary>
        /// Fetches a User instance from the data store, by Username
        /// </summary>
        /// <param name="username">Username (PK) to fetch</param>
        /// <param name="addIfMissing">If true, will attempt to create a new user if there is no match</param>
        /// <returns>a User if one is found or created, otherwise null</returns>
        public User FetchUser(string username, bool addIfMissing)
        {
            if (String.IsNullOrEmpty(username))
                throw new ArgumentException("Username is required.");

            if (addIfMissing)
                return _users.GetOrAdd(username, AddUser);

            if (_users.TryGetValue(username, out User u))
                return u;
            else
                return null;

        }

        /// <summary>
        /// Given a username, creates a new User instance with pseudo-random initial values
        /// and writes it to the data store.
        /// </summary>
        /// <param name="username">Username (PK)</param>
        /// <returns>A new User with random-ish values</returns>
        public User AddUser(string username)
        {
            if (String.IsNullOrEmpty(username))
                throw new ArgumentException("Username is required.");

            User u = RandomUser(username);
            return AddUser(u);
        }

        /// <summary>
        /// Adds an already-created User object to the data store
        /// </summary>
        /// <param name="usr">User to add</param>
        /// <returns>The same user (refetched after write)</returns>
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
            //just pick a day from earlier in 2020
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
