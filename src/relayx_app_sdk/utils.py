import inspect


async def invoke_callback(cb, *args):
    """Call a callback, awaiting it if it's an async function."""
    if inspect.iscoroutinefunction(cb):
        return await cb(*args)
    else:
        return cb(*args)


def build_credentials(api_key, secret):
    jwt = api_key.strip()
    seed = secret.strip()

    return (
        f"-----BEGIN NATS USER JWT-----\n"
        f"{jwt}\n"
        f"------END NATS USER JWT------\n"
        f"\n"
        f"************************* IMPORTANT *************************\n"
        f"NKEY Seed printed below can be used to sign and prove identity.\n"
        f"NKEYs are sensitive and should be treated as secrets.\n"
        f"\n"
        f"-----BEGIN USER NKEY SEED-----\n"
        f"{seed}\n"
        f"------END USER NKEY SEED------\n"
        f"\n"
        f"*************************************************************"
    )


def topic_pattern_matcher(pattern_a, pattern_b):
    a = pattern_a.split('.')
    b = pattern_b.split('.')

    i, j = 0, 0
    star_ai, star_aj = -1, -1
    star_bi, star_bj = -1, -1

    while i < len(a) or j < len(b):
        tok_a = a[i] if i < len(a) else None
        tok_b = b[j] if j < len(b) else None

        if tok_a == '>':
            if i != len(a) - 1:
                return False
            if j >= len(b):
                return False

            star_ai = i
            i += 1
            j += 1
            star_aj = j
            continue

        if tok_b == '>':
            if j != len(b) - 1:
                return False
            if i >= len(a):
                return False

            star_bi = j
            j += 1
            i += 1
            star_bj = i
            continue

        single_wildcard = (
            (tok_a == '*' and j < len(b)) or
            (tok_b == '*' and i < len(a))
        )

        if (tok_a is not None and tok_a == tok_b) or single_wildcard:
            i += 1
            j += 1
            continue

        if star_ai != -1:
            star_aj += 1
            j = star_aj
            continue

        if star_bi != -1:
            star_bj += 1
            i = star_bj
            continue

        return False

    return True
