using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Challenge;
using System.Net.Http;
using ChallengeWeb.Repository;

namespace ChallengeWeb.Controllers
{
    [Route("api/user")]
    [ApiController]
    public class UsersController : ControllerBase
    {
        private IUserRepository Repo { get; }

        public UsersController(IUserRepository userRepo)
        {
            Repo = userRepo;
        }

        [HttpGet("{username}")]
        public IActionResult Get(string username)
        {
            //as per spec, tell the repo to create a new user 
            //if one doesn't already exist for the username
            User u = Repo.FetchUser(username, true);
            if (null != u)
            {
                return new ObjectResult(u);
            }
            else
            {
                return NotFound();
            }
        }

       
    }
}
