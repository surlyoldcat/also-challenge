using System;
using System.Collections.Generic;
using System.Text;
using AE.CoreInterface;
using AE.CoreUtility;

namespace Challenge
{
    public class User : IOBinary
    {
        public string Username { get; set; }
        public string FavoriteColor { get; set; }
        public DateTime DateCreated { get; set; }
        public string TimeZone { get; set; }
        public PermissionSet Permissions { get; } = new PermissionSet();

        public byte[] IO {
            get
            {
                string[] map = new string[] { "NVarChar", "NVarChar", "DateTime2", "NVarChar", "VarBinary" };
                object[] vals = new object[] { Username, FavoriteColor, DateCreated, TimeZone, Permissions.IO };
                var bio = new BlobIO(map, vals);
                return bio.IO;

            }
            set
            {
                var bio = new BlobIO(value);
                Username = bio.GetString(0);
                FavoriteColor = bio.GetString(1);
                DateCreated = bio.GetDateTime(2).Value;
                TimeZone = bio.GetString(3);
                Permissions.IO = bio.GetBytes(4);
            }
        }

        // this should do something more intelligent, but that might require attempting to serialize the whole object
        public bool IOOK  => IO?.Length > 0;
    }
}
