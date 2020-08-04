def validate_trajectory(ds_traj):
    required_fields = ["lat", "lon", "time", "u_traj", "v_traj"]
    missing_fields = list(filter(lambda f: f not in ds_traj, required_fields))

    if len(missing_fields) > 0:
        raise Exception(
            "The provided trajectory is missing the following"
            " fields: {}".format(", ".join(missing_fields))
        )


def validate_forcing_profiles(ds_forcing_profiles):
    FORCING_VARS = ["q", "t", "u", "v"]
    required_fields = ["lat", "lon", "time", "level"]
    for v in FORCING_VARS:
        required_fields += [f"{v}_mean", f"{v}_local", f"d{v}dt_adv"]

    missing_fields = list(
        filter(lambda f: f not in ds_forcing_profiles, required_fields)
    )

    if len(missing_fields) > 0:
        raise Exception(
            "The provided forcing profiles are missing the"
            " following fields: {}".format(", ".join(missing_fields))
        )
