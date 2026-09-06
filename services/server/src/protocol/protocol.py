from lottery import Bet

BYTE_ORDER = "big"

TYPE_BYTES = 2
SIZE_BYTES = 2

BET_BYTES = 1
ENDOFBETS_BYTES = 2
ACK_BYTES = 3
BATCH_BYTES = 4

AGENCYID_BYTES = 1
FIRSTNAME_BYTES = 2
LASTNAME_BYTES = 3
DOCUMENT_BYTES = 4
BIRTHDATE_BYTES = 5
NUMBER_BYTES = 6


class EndOfBets:
    def __init__(self, agency_id):
        self.agency_id = agency_id


def parse_header(header):
    msg_type = int.from_bytes(header[:TYPE_BYTES], BYTE_ORDER)
    size = int.from_bytes(header[TYPE_BYTES:], BYTE_ORDER)
    return msg_type, size


def split_msg(data):
    msg_type, size = parse_header(data[: TYPE_BYTES + SIZE_BYTES])
    total_len = TYPE_BYTES + SIZE_BYTES + size
    return msg_type, data[:total_len], data[total_len:]


def decode_bet(value):
    agency_id = None
    first_name = None
    last_name = None
    document = None
    birthdate = None
    number = None

    while len(value) > 0:
        field_type, field_msg, rest = split_msg(value)
        field_value = field_msg[TYPE_BYTES + SIZE_BYTES :]

        if field_type == AGENCYID_BYTES:
            agency_id = field_value
        elif field_type == FIRSTNAME_BYTES:
            first_name = field_value
        elif field_type == LASTNAME_BYTES:
            last_name = field_value
        elif field_type == DOCUMENT_BYTES:
            document = field_value
        elif field_type == BIRTHDATE_BYTES:
            birthdate = field_value
        elif field_type == NUMBER_BYTES:
            number = field_value

        value = rest

    return Bet(
        agency_id=int(agency_id.decode()),
        first_name=first_name.decode(),
        last_name=last_name.decode(),
        document=int(document.decode()),
        birthdate=birthdate.decode(),
        number=int(number.decode()),
    )


def decode_batch(value):
    bets = []
    while len(value) > 0:
        _, msg, rest = split_msg(value)
        bets.append(decode_bet(msg[TYPE_BYTES + SIZE_BYTES :]))
        value = rest
    return bets


def parse_message(msg_type, value):
    if msg_type == BET_BYTES:
        return decode_bet(value)
    elif msg_type == ENDOFBETS_BYTES:
        return EndOfBets(agency_id=int(value.decode()))
    elif msg_type == BATCH_BYTES:
        return decode_batch(value)


def encode_field(field_type, value):
    header = field_type.to_bytes(TYPE_BYTES, BYTE_ORDER) + len(value).to_bytes(SIZE_BYTES, BYTE_ORDER)
    return header + value


def encode_bet(bet):
    value = encode_field(AGENCYID_BYTES, str(bet.agency_id).encode())
    value += encode_field(FIRSTNAME_BYTES, bet.first_name.encode())
    value += encode_field(LASTNAME_BYTES, bet.last_name.encode())
    value += encode_field(DOCUMENT_BYTES, str(bet.document).encode())
    value += encode_field(BIRTHDATE_BYTES, bet.birthdate.encode())
    value += encode_field(NUMBER_BYTES, str(bet.number).encode())
    return encode_field(BET_BYTES, value)


def encode_batch(bets):
    value = b"".join(encode_bet(bet) for bet in bets)
    return encode_field(BATCH_BYTES, value)
