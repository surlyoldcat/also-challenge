
# Instructions
- You will need Visual Studio to complete challenges outlined below. We suggest you utilize one of the community editions provided at https://www.visualstudio.com/downloads/
- Clone this repo to your local machine
- Use it to create a new repo on GitHub under your own account (please don't use GitHub fork to accomplish this)
- Complete the challenge below or provide an alternative representative sample of code or classes that is solely your work product. 
  (If you maintain a github project where you are the sole contributor, please feel free to submit a link and description of what we should review in the repository.)
- Send us an email with a link to your repo and any instructions or details you want to share about key features, performance optimizations or creative problem solving skills that they exemplify.

## Also Energy Code Challenge

### Please review the projects in the Challenge solution and perform the following tasks. Please feel free to be creative and simplify when possible. 

1) Add a PermissionSet class to hold a fixed list of 100 user permissions (i.e. perm1, perm2, ...). 
   The PermissionSet should be able to serialize as a byte array.

2) Add the ability for the BlobIO class to append and retrieve the new user permission class.

3) Add a User class that serializes as a BlobIO and includes username, permissions, create date, timezone, and favorite color.

4) Fix the bug causing the existing test to fail and add a new unit test to verify the functionality of steps 1 through 3.

5) Add a web or api project with an endpoint that returns the user details when api/user/{username} is requested.
   The endpoint should generate random values on a new user if the username has not been previously requested but once retrieved, the same user properties should result from subsequent calls.
   Persistance beyond an app restart is not required.

6) Provide a brief comment for each of the following:

   - Concept or element that was unfamiliar or unexpected

     > RKT: My guess is that BlobIO is intended for use in SQL Server stored procs. This is a feature I never got around to using, because it was always forbidden by the DBA team.  It's also the first time I've seen custom binary serialization used for writing to a DB.


   - Constructive review or recommended improvement

     > RKT: I generally prefer to stay away from operator overloading and stick to the more verbose OO way of naming methods, but if this is to be consumed inside of stored procs, I can see why it could be advantageous. One thing I'm a little more concerned about is 'properties' that have lots of side-effects and mutate objects' internal state in multiple ways. I think I'd prefer to have the BinaryIO interface implemented with regular methods, rather than properties. 


   - Opportunity or future enhancement

     

     > RKT: It might be beneficial to split BlobIO into multiple classes, to segregate state management vs utility methods. Also, it would help to break up the single unit test into multiple feature-specific and data-driven tests, to make it easier to pinpoint potential issues.