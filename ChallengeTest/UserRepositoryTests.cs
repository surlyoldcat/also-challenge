using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using ChallengeWeb.Repository;
using Challenge;
using System.Runtime.CompilerServices;

namespace ChallengeTest
{
    public class UserRepositoryTests
    {
        [Fact]
        public void UserGetOrCreateTest()
        {
            string email = "foo@bar.com";

            IUserRepository repo = new UserRepository();
            var usr = repo.FetchUser(email, true);
            Assert.NotNull(usr);
            Assert.False(String.IsNullOrEmpty(usr.FavoriteColor));
            Assert.False(String.IsNullOrEmpty(usr.TimeZone));
            Assert.Equal(email, usr.Username);

            var usr2 = repo.FetchUser(email, true);
            Assert.Equal(usr.DateCreated, usr2.DateCreated);
            Assert.Equal(usr.TimeZone, usr2.TimeZone);
            Assert.Equal(usr.FavoriteColor, usr2.FavoriteColor);

        }

        [Fact]
        public void UserGetOrNullTest()
        {
            string email = "jim@wolfenstein.org";

            IUserRepository repo = new UserRepository();
            var usr = repo.FetchUser(email, false);
            Assert.Null(usr);
        }


        [Fact]
        public void UserAddTest()
        {
            string email = "joe@googly.com";
            string tz = TimeZoneInfo.Local.Id;
            var u = new User
            {
                Username = email,
                FavoriteColor = "Black",
                TimeZone = tz,
                DateCreated = DateTime.Now
            };
            u.Permissions[PermissionIndex.Perm10] = true;

            IUserRepository repo = new UserRepository();
            var u2 = repo.AddUser(u);

            Assert.Equal(u2.Username, u.Username);
            Assert.Equal(u2.TimeZone, u.TimeZone);
            Assert.True(u2.Permissions[PermissionIndex.Perm10]);


        }
    }
}
