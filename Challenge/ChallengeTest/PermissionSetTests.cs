using System;
using System.Collections.Generic;
using System.Text;
using Challenge;
using Xunit;

namespace ChallengeTest
{
    public class PermissionSetTests
    {
        [Fact]
        public void TestPermissionSetSerialize()
        {
            var ps = new PermissionSet();
            Assert.False(ps[PermissionIndex.Perm1]);
            Assert.False(ps[PermissionIndex.Perm55]);
            ps[PermissionIndex.Perm12] = true;
            Assert.True(ps[PermissionIndex.Perm12]);
            ps[PermissionIndex.Perm1] = true;
            Assert.True(ps[PermissionIndex.Perm1]);
            ps[PermissionIndex.Perm100] = true;
            Assert.True(ps[PermissionIndex.Perm100]);

            byte[] psData = ps.IO;

            var ps2 = new PermissionSet();
            ps2.IO = psData;
            Assert.False(ps2[PermissionIndex.Perm55]);
            Assert.True(ps2[PermissionIndex.Perm12]);
            Assert.True(ps2[PermissionIndex.Perm1]);
            Assert.True(ps2[PermissionIndex.Perm100]);

            ps2[PermissionIndex.Perm10] = true;

            var ps3 = new PermissionSet(ps2.IO);
            Assert.True(ps2[PermissionIndex.Perm12]);
            Assert.True(ps2[PermissionIndex.Perm1]);
            Assert.True(ps2[PermissionIndex.Perm100]);
            Assert.True(ps2[PermissionIndex.Perm10]);


        }
    }
}
