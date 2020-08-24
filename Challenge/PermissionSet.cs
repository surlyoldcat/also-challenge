using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AE.CoreInterface;
using AE.CoreUtility;

namespace Challenge
{
    public class PermissionSet : IOBinary
    {
        public const int SET_LENGTH = 100;
        protected readonly BigBitBlob permissions = new BigBitBlob(SET_LENGTH);

        public PermissionSet()
        {
            //init new permissions by setting all to false
            foreach(int i in Enum.GetValues(typeof(PermissionIndex)))
            {
                permissions[i] = false;
            }
        }

        public PermissionSet(byte[] bitBlob)
        {
            this.IO = bitBlob;

        }

        public bool this[PermissionIndex p]
        {
            get { return permissions[(int)p].Value; }
            set { permissions[(int)p] = value; }
        }

        public byte[] IO 
        {
            get
            {
                return permissions.IO;
            }
            set
            {
                permissions.IO = value;
            }
        }
        public bool IOOK
        {
            get
            {
                return permissions.IOOK;
            }

        }

    }
}
