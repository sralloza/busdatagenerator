from dataclasses import dataclass


@dataclass(order=True)
class Coord:
    x: float

    def __sub__(self, other):
        assert isinstance(other, Coord)
        return self.x - other.x

    def __add__(self, other):
        assert isinstance(other, Coord)
        return self.x + other.x


@dataclass
class Groupable:
    coord: Coord

    def __lt__(self, other):
        assert isinstance(other, Groupable)
        return self.coord < other.coord


class Grouper(list):
    def _distance(self, x: Groupable, y: Groupable):
        return abs(x.coord - y.coord)

    def group(self, epsilon, selector=max):

        try:
            _ = self[0].coord
        except AttributeError:
            raise ValueError('Members of grouper are not Groupable')
        except IndexError:
            raise ValueError('Empty grouper')

        self.sort(key=lambda x: x.coord.x)

        groups = []
        temp_group = []
        i = 0
        k = 0
        while i < len(self):
            k = i
            while self._distance(self[i], self[k]) < epsilon:
                temp_group.append(self[k])
                k += 1
                if k == len(self):
                    break

            # print(self._distance(self[i], self[i + 1]))
            i = k
            groups.append(temp_group)
            temp_group = []

        self.__init__([selector(x) for x in groups])

    def __str__(self):
        return '\n'.join((repr(x) for x in self))


def test():
    test = (
        # Groupable(Coord(random.randint(0, 50))),
        Groupable(coord=Coord(x=1)),
        Groupable(coord=Coord(x=2)),
        Groupable(coord=Coord(x=5)),
        Groupable(coord=Coord(x=5)),
        Groupable(coord=Coord(x=6)),
        Groupable(coord=Coord(x=8)),
        Groupable(coord=Coord(x=9)),
        Groupable(coord=Coord(x=13)),
        Groupable(coord=Coord(x=13)),
        Groupable(coord=Coord(x=13)),
        Groupable(coord=Coord(x=14)),
        Groupable(coord=Coord(x=14)),
        Groupable(coord=Coord(x=14)),
        Groupable(coord=Coord(x=16)),
        Groupable(coord=Coord(x=16)),
        Groupable(coord=Coord(x=16)),
        Groupable(coord=Coord(x=18)),
        Groupable(coord=Coord(x=18)),
        Groupable(coord=Coord(x=21)),
        Groupable(coord=Coord(x=21)),
        Groupable(coord=Coord(x=21)),
        Groupable(coord=Coord(x=22)),
        Groupable(coord=Coord(x=22)),
        Groupable(coord=Coord(x=24)),
        Groupable(coord=Coord(x=24)),
        Groupable(coord=Coord(x=25)),
        Groupable(coord=Coord(x=29)),
        Groupable(coord=Coord(x=32)),
        Groupable(coord=Coord(x=34)),
        Groupable(coord=Coord(x=35)),
        Groupable(coord=Coord(x=37)),
        Groupable(coord=Coord(x=38)),
        Groupable(coord=Coord(x=41)),
        Groupable(coord=Coord(x=45)),
        Groupable(coord=Coord(x=46)),
        Groupable(coord=Coord(x=48)),
    )

    grouper = Grouper(test)
    grouper.group(10)
    print(grouper)


if __name__ == '__main__':
    test()
