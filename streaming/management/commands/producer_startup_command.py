from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from ...kafkaProducer import producer

class Command(BaseCommand):
    help = 'My custom startup command'

    def handle(self, *args, **kwargs):
        try:
            time = timezone.now().strftime('%X')
            self.stdout.write("It's now %s" % time)
        except:
            raise CommandError('Initalization failed.')