using System;
using System.Linq;
using System.Text;
using AE.CoreUtility;
using Challenge;
using Xunit;

namespace ChallengeTest
{
    public class UserTests
    {
        [Fact]
        public void TestUserSerialize()
        {
            User usr = new User
            {
                Username = "foo@bar.com",
                FavoriteColor = "Beige",
                DateCreated = DateTime.Now,
                TimeZone = TimeZoneInfo.Local.Id

            };
            usr.Permissions[PermissionIndex.Perm1] = true;
            usr.Permissions[PermissionIndex.Perm15] = true;
            usr.Permissions[PermissionIndex.Perm100] = true;

            byte[] userSer = usr.IO;

            BlobIO bio = new BlobIO(usr.IO);
            Assert.True(bio.PK == 0);

            User usr2 = new User();
            usr2.IO = userSer;

            Assert.Equal(usr.Username, usr2.Username);
            Assert.Equal(usr.FavoriteColor, usr2.FavoriteColor);
            Assert.Equal(usr.DateCreated, usr2.DateCreated);
            Assert.Equal(usr.TimeZone, usr2.TimeZone);
            Assert.True(usr2.Permissions[PermissionIndex.Perm1]);
            Assert.True(usr2.Permissions[PermissionIndex.Perm15]);
            Assert.True(usr2.Permissions[PermissionIndex.Perm100]);
            Assert.False(usr2.Permissions[PermissionIndex.Perm23]);

        }

        
    }
}
