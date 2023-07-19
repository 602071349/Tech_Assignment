from tech_code import check_valid


def test_check_valid():
    valid_address = "1PPkPubRnK2ry9PPVW7HJiukqbSnWzXkbi"
    invalid_address = "abc123"

    assert(check_valid(valid_address) == True)

    assert (check_valid(invalid_address) == False)
