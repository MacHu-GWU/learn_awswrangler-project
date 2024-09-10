# -*- coding: utf-8 -*-

from learn_awswrangler import api


def test():
    _ = api


if __name__ == "__main__":
    from learn_awswrangler.tests import run_cov_test

    run_cov_test(__file__, "learn_awswrangler.api", preview=False)
