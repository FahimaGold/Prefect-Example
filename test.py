from prefect import flow, task
import httpx


@task(retries=3, retry_delay_seconds=60) # Prefect will now auto-retry if GitHub blocks you!
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    response = httpx.get(url)
    
    # Check if the request actually worked
    if response.status_code != 200:
        print(f"Error from GitHub: {response.text}")
        raise ValueError(f"GitHub returned {response.status_code}")
        
    data = response.json()
    count = data.get("stargazers_count", 0) # .get() prevents the KeyError crash
    print(f"{repo} has {count} stars!")
    return count


@flow(name="GitHub Stars")
def github_stars(repos: list[str]):
    for repo in repos:
        get_stars(repo)


# run the flow!
if __name__ == "__main__":
    github_stars(["PrefectHQ/prefect"])