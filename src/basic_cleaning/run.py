#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply som basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb

import os
import tempfile
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################

    # Connect to W&B and load input artifact
    logger.info(f"Downloading and reading artifact: {args.input_artifact}")
    run = wandb.init(project="nyc_airbnb", group="eda", save_code=True)
    local_path = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(local_path).set_index("id")
    

    # Remove outliers
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Turn columns to an specific type
    df["last_review"] = pd.to_datetime(df["last_review"])


    logger.info(f"Uploading output artifact {args.output_artifact} to W&B")
    with tempfile.TemporaryDirectory() as tmp_dir:

        temp_path = os.path.join(tmp_dir, args.output_artifact)

        # Save preprocessed data in temp directory
        df.to_csv(temp_path)

        # Upload output artifact to W&B
        artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
        )
        artifact.add_file(temp_path)
        run.log_artifact(artifact)

        # Wait until data has been loaded successfully to W&B
        artifact.wait()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Enter the name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Enter the name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Enter the type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Add brief description of the output file",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum value allowed for price attribute",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum value allowed for price attribute",
        required=True
    )


    args = parser.parse_args()

    go(args)
