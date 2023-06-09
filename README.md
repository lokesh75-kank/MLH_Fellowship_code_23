# Machine learning Assignment Explanation

I have completed the assignment as part of my Machine learning Engineering course at San Diego State University

Dataset: I have loaded the Baseball dataset directly from MariaDB into Spark, as required by the task. I ensured that only the necessary raw data was extracted from MariaDB, and no calculations were done in the database. All the logic for the task was implemented in Spark.

Batting Average Calculation: I have implemented a Transformer in Spark to calculate the batting average for the players in the dataset. The calculation is performed using a rolling window of the last 100 days, as required by the task.

Rolling Window: To look at the last 100 days that a player was in prior to a game, I have reused the logic that I implemented earlier. I made sure that the calculation was working as expected before wrapping it up as a transformer.

Pull Request: I have worked outside of my master branch and have created a pull request for the hw_03 branch. I have asked my buddy to review the pull request before sending it to the project owner.

Overall, I have successfully completed all the tasks assigned to me for this project. I have loaded the dataset into Spark directly from MariaDB, calculated the batting average using a transformer with a rolling window of the last 100 days, and have followed the best practices of working outside of the master branch and creating a pull request for review before sending it to the project owner.

# Learning

Through this task, I learned how to use Apache Spark to process large datasets and calculate metrics such as batting averages over a rolling window. I also gained experience in working with a MariaDB database and loading data directly into Spark.

In addition to technical skills, I also learned about best practices for collaborative coding, such as working outside of the master branch and creating pull requests for review by a buddy. This ensures that the code is thoroughly reviewed and that any issues or improvements are addressed before it is merged into the main codebase.

Overall, this task provided me with valuable experience in working with Spark and databases, as well as collaborative coding practices that are essential for working in a team. I have also developed problem-solving skills in finding ways to optimize the processing of large datasets and implementing the necessary logic to calculate metrics such as batting averages.

# Python Project Structure

# Setup for developement:

- Setup a python 3.x venv (usually in `.venv`)
  - You can run `./scripts/create-venv.sh` to generate one
- `pip3 install --upgrade pip`
- Install pip-tools `pip3 install pip-tools`
- Update dev requirements: `pip-compile --output-file=requirements.dev.txt requirements.dev.in`
- Update requirements: `pip-compile --output-file=requirements.txt requirements.in`
- Install dev requirements `pip3 install -r requirements.dev.txt`
- Install requirements `pip3 install -r requirements.txt`
- `pre-commit install`

## Update versions

`pip-compile --output-file=requirements.dev.txt requirements.dev.in --upgrade`
`pip-compile --output-file=requirements.txt requirements.in --upgrade`

# Run `pre-commit` locally.

`pre-commit run --all-files`
