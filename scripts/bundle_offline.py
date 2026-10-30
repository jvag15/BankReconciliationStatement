import os
import subprocess
from pathlib import Path

def run_cmd(cmd):
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def main():
    root_dir = Path(__file__).parent.parent
    offline_dir = root_dir / "offline"
    
    # 1. Create directories
    for sub in ["docker-images", "config", "scripts", "models"]:
        (offline_dir / sub).mkdir(parents=True, exist_ok=True)
        
    print("Building local images via docker-compose...")
    # Change cwd to root so compose finds the file
    os.chdir(root_dir)
    run_cmd("docker-compose build")
    
    # In Compose V2, project names are often normalized. We'll tag them to be safe.
    # The default project name is usually the directory name, 'bank-statement'.
    # Sometimes it's 'bank-statement-brs_web', sometimes 'bank_statement-brs_web'.
    # We will let compose handle the build, then we can pull third party ones.
    
    # Let's inspect docker-compose images to be precise, or just use docker image ls.
    # To be perfectly stable, we can explicitly build and tag them outside compose:
    print("Explicitly building and tagging local images...")
    run_cmd("docker build -t brs_web:offline .")
    run_cmd("docker build -t brs_worker:offline .")
    run_cmd("docker build -t brs_nginx:offline -f Dockerfile.client .")
    
    images_to_save = {
        "brs_web.tar": "brs_web:offline",
        "brs_worker.tar": "brs_worker:offline",
        "brs_nginx.tar": "brs_nginx:offline",
        "postgres_15.tar": "postgres:15-alpine",
        "redis_7.tar": "redis:7-alpine",
        "prometheus.tar": "prom/prometheus:latest",
        "grafana.tar": "grafana/grafana:latest",
        "celery_exporter.tar": "danihodovic/celery-exporter:latest"
    }
    
    print("Pulling third-party images...")
    for tag in images_to_save.values():
        if "offline" not in tag:
            run_cmd(f"docker pull {tag}")
            
    print("Saving images to offline bundle...")
    for tar_name, tag in images_to_save.items():
        tar_path = offline_dir / "docker-images" / tar_name
        print(f"Exporting {tag} to {tar_path} ...")
        run_cmd(f"docker save -o \"{tar_path}\" {tag}")
            
    print("Offline bundle docker images generated successfully!")

if __name__ == "__main__":
    main()
