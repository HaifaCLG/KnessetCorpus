class Sentence:
    def __init__(self, sentence_id, sentence_text, protocol_name, turn_num_in_protocol, sent_num_in_turn):
        self.sentence_id = sentence_id
        self.protocol_name  = protocol_name
        self.speaker_id = ''
        self.speaker_name = ''
        self.is_valid_speaker = True
        self.turn_num_in_protocol = turn_num_in_protocol
        self.sent_num_in_turn = sent_num_in_turn
        self.sentence_text = sentence_text
        self.is_chairman = False
        self.morphological_fields = None
        self.factuality_fields = None
