from unittest import TestCase
from models.protocol_models import get_clean_speaker_name

class Test(TestCase):
    def test_get_clean_speaker_name(self):
        name = 'ח"כ אורנה בנאי(העבודה)'
        res_name = 'אורנה בנאי'
        self.assertEqual(res_name, get_clean_speaker_name(name))
        name = 'ח"כ אורנה בנאי (העבודה)'
        res_name = 'אורנה בנאי'
        self.assertEqual(res_name, get_clean_speaker_name(name))
        name = 'חה״כ אורנה בן-חיים'
        res_name = 'אורנה בן-חיים'
        self.assertEqual(res_name, get_clean_speaker_name(name))
        name = 'יו"ר אורנה חכים'
        res_name = 'אורנה חכים'
        self.assertEqual(res_name, get_clean_speaker_name(name))
        name = 'בלה בלה'
        res_name = 'בלה בלה'
        self.assertEqual(res_name, get_clean_speaker_name(name))
        name = 'בלה בלה'
        res_name = 'בלה'
        self.assertNotEqual(res_name, get_clean_speaker_name(name))



test = Test()
test.test_get_clean_speaker_name()